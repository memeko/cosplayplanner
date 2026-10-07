package ru.cosplayplanner.mobile.data

import com.squareup.moshi.Moshi
import com.squareup.moshi.Types
import java.time.LocalDate
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.flow.Flow
import ru.cosplayplanner.mobile.data.local.*
import ru.cosplayplanner.mobile.data.model.*
import ru.cosplayplanner.mobile.data.remote.MobileApi

@Singleton
class SyncRepository @Inject constructor(
    private val api: MobileApi,
    private val db: AppDatabase,
    moshi: Moshi,
) {
    private val mapAdapter = moshi.adapter<Map<String, Any?>>(
        Types.newParameterizedType(Map::class.java, String::class.java, Any::class.java),
    )

    fun cards(): Flow<List<CosplanCardEntity>> = db.cardDao().observeAll()
    fun inProgress(): Flow<List<InProgressEntity>> = db.inProgressDao().observeAll()
    fun pigeons(): Flow<List<PigeonMessageEntity>> = db.pigeonDao().observeAll()
    fun festivals(): Flow<List<FestivalEntity>> {
        val today = LocalDate.now()
        return db.festivalDao().observeOfflineWindow(today.toString(), today.plusDays(30).toString())
    }

    suspend fun login(login: String, password: String): MobileLoginResponse {
        val normalizedLogin = login.trim()
        val mobileResponse = api.mobileLogin(MobileLoginRequest(normalizedLogin, password))
        if (mobileResponse.isSuccessful) {
            return mobileResponse.body() ?: MobileLoginResponse(false, null, "Сервер вернул пустой ответ.")
        }
        if (mobileResponse.code() != 404) {
            return MobileLoginResponse(false, null, "Ошибка авторизации: HTTP ${mobileResponse.code()}")
        }

        // Compatibility path for servers where mobile bootstrap is deployed,
        // but the dedicated JSON login endpoint has not been released yet.
        val webResponse = api.webLogin(normalizedLogin, password)
        val finalPath = webResponse.raw().request.url.encodedPath
        val success = webResponse.isSuccessful && finalPath != "/login"
        return MobileLoginResponse(
            ok = success,
            user = null,
            detail = if (success) null else "Неверный логин или пароль.",
        )
    }

    suspend fun bootstrap(since: String? = null) {
        val response = api.bootstrap(since)
        if (!response.ok) return
        db.userDao().upsert(UserProfileEntity(response.user.id, response.user.username, response.user.cosplayNick, response.user.email, response.user.homeCity))
        db.cardDao().upsertAll(response.cards.map { CosplanCardEntity(it.id, it.updatedAt, mapAdapter.toJson(it.payload)) })
        db.festivalDao().upsertAll(response.festivals.map {
            FestivalEntity(it.id, it.updatedAt, mapAdapter.toJson(it.payload), it.payload["event_date"] as? String, it.payload["is_going"] as? Boolean ?: false)
        })
        db.inProgressDao().upsertAll(response.inProgress.map { InProgressEntity(it.id, it.cardId, it.updatedAt, mapAdapter.toJson(it.payload)) })
    }

    suspend fun editCard(item: CosplanCardEntity, updates: Map<String, Any?>) {
        val payload = (mapAdapter.fromJson(item.payloadJson) ?: emptyMap()).toMutableMap().apply { putAll(updates) }
        db.cardDao().upsert(item.copy(payloadJson = mapAdapter.toJson(payload)))
        enqueue("card", item.id, item.updatedAt, payload)
    }

    suspend fun editInProgress(item: InProgressEntity, updates: Map<String, Any?>) {
        val payload = (mapAdapter.fromJson(item.payloadJson) ?: emptyMap()).toMutableMap().apply { putAll(updates) }
        db.inProgressDao().upsert(item.copy(payloadJson = mapAdapter.toJson(payload)))
        enqueue("in_progress", item.id, item.updatedAt, payload)
    }

    private suspend fun enqueue(scope: String, id: Long, baseUpdatedAt: String?, payload: Map<String, Any?>) {
        db.syncQueueDao().upsert(SyncQueueEntity(UUID.randomUUID().toString(), scope, id, baseUpdatedAt, mapAdapter.toJson(payload), System.currentTimeMillis()))
    }

    suspend fun syncNow() {
        pushPendingChanges()
        bootstrap()
        refreshPigeons()
    }

    suspend fun pushPendingChanges() {
        val queue = db.syncQueueDao().getBatch(100)
        if (queue.isEmpty()) return
        fun request(item: SyncQueueEntity) = SyncEntityRequest(item.clientUid, item.entityId, item.baseUpdatedAt, true, "client_wins", mapAdapter.fromJson(item.payload) ?: emptyMap())
        val done = linkedSetOf<String>()
        val normal = queue.filter { it.scope != "in_progress" }
        if (normal.isNotEmpty()) {
            val response = api.sync(MobileSyncRequest(normal.filter { it.scope == "card" }.map(::request), normal.filter { it.scope == "festival" }.map(::request)))
            if (response.ok) (response.cards + response.festivals).filter { it.status in setOf("applied", "skipped_server_wins") }.mapNotNullTo(done) { it.clientUid }
        }
        val progress = queue.filter { it.scope == "in_progress" }
        if (progress.isNotEmpty()) {
            val response = api.syncInProgress(MobileInProgressSyncRequest(progress.map(::request)))
            if (response.ok) response.items.filter { it.status in setOf("applied", "skipped_server_wins") }.mapNotNullTo(done) { it.clientUid }
        }
        if (done.isNotEmpty()) db.syncQueueDao().deleteByIds(done.toList())
    }

    suspend fun refreshPigeons() {
        val response = api.pigeons()
        if (response.ok) db.pigeonDao().upsertAll(response.messages.map { PigeonMessageEntity(it.id, it.chatUserId, it.chatAlias, it.direction, it.body, it.createdAt, it.isRead) })
    }

    suspend fun sendPigeon(alias: String, body: String) {
        val response = api.sendPigeon(PigeonSendRequest(alias.trim(), body.trim()))
        if (!response.ok) error(response.detail ?: "Не удалось отправить голубя")
        refreshPigeons()
    }

    fun decode(json: String): Map<String, Any?> = mapAdapter.fromJson(json) ?: emptyMap()
}
