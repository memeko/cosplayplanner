package ru.cosplayplanner.mobile.data

import com.squareup.moshi.Moshi
import com.squareup.moshi.Types
import java.time.LocalDate
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
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
    private val anyAdapter = moshi.adapter(Any::class.java)
    private val _homeCity = MutableStateFlow<String?>(null)
    val homeCity: StateFlow<String?> = _homeCity

    fun cards(): Flow<List<CosplanCardEntity>> = db.cardDao().observeAll()
    fun inProgress(): Flow<List<InProgressEntity>> = db.inProgressDao().observeAll()
    fun pigeons(): Flow<List<PigeonMessageEntity>> = db.pigeonDao().observeAll()
    fun calendarEvents(): Flow<List<CalendarEventEntity>> = db.calendarDao().observeEvents()
    fun contentPosts(): Flow<List<ContentPostEntity>> = db.calendarDao().observePosts()
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
        _homeCity.value = response.user.homeCity
        db.cardDao().upsertAll(response.cards.map { CosplanCardEntity(it.id, it.updatedAt, mapAdapter.toJson(it.payload)) })
        db.festivalDao().upsertAll(response.festivals.map {
            FestivalEntity(it.id, it.updatedAt, mapAdapter.toJson(it.payload), it.payload["event_date"] as? String, it.payload["is_going"] as? Boolean ?: false)
        })
        db.inProgressDao().upsertAll(response.inProgress.map { InProgressEntity(it.id, it.cardId, it.updatedAt, mapAdapter.toJson(it.payload)) })
        db.calendarDao().upsertEvents(response.calendarEvents.map { CalendarEventEntity(it.id, it.date, it.time, it.title, it.city, it.details, it.updatedAt) })
        db.calendarDao().upsertPosts(response.contentPosts.map { ContentPostEntity(it.id, it.date, it.time, it.title, it.description, anyAdapter.toJson(it.socials), it.rubric, it.status, it.published, it.updatedAt) })
    }

    suspend fun editCard(item: CosplanCardEntity, updates: Map<String, Any?>) {
        val payload = (mapAdapter.fromJson(item.payloadJson) ?: emptyMap()).toMutableMap().apply { putAll(updates) }
        db.cardDao().upsert(item.copy(payloadJson = mapAdapter.toJson(payload)))
        enqueueOrReplaceLocal("card", item.id, item.updatedAt, payload)
    }

    suspend fun createCard(payload: Map<String, Any?>) {
        val localId = -System.currentTimeMillis()
        db.cardDao().upsert(CosplanCardEntity(localId, null, mapAdapter.toJson(payload)))
        enqueue("card", localId, null, payload)
    }

    suspend fun editInProgress(item: InProgressEntity, updates: Map<String, Any?>) {
        val payload = (mapAdapter.fromJson(item.payloadJson) ?: emptyMap()).toMutableMap().apply { putAll(updates) }
        db.inProgressDao().upsert(item.copy(payloadJson = mapAdapter.toJson(payload)))
        enqueueOrReplaceLocal("in_progress", item.id, item.updatedAt, payload)
    }

    suspend fun createInProgress(cardId: Long) {
        val localId = -System.currentTimeMillis()
        val payload = mapOf<String, Any?>("card_id" to cardId, "checklist_json" to emptyList<Any>(), "task_rows_json" to emptyList<Any>(), "is_frozen" to false)
        db.inProgressDao().upsert(InProgressEntity(localId, cardId, null, mapAdapter.toJson(payload)))
        enqueue("in_progress", localId, null, payload)
    }

    private suspend fun enqueue(scope: String, id: Long, baseUpdatedAt: String?, payload: Map<String, Any?>) {
        db.syncQueueDao().upsert(SyncQueueEntity(UUID.randomUUID().toString(), scope, id, baseUpdatedAt, mapAdapter.toJson(payload), System.currentTimeMillis()))
    }

    private suspend fun enqueueOrReplaceLocal(scope: String, id: Long, baseUpdatedAt: String?, payload: Map<String, Any?>) {
        val existing = if (id < 0) db.syncQueueDao().findForLocalEntity(scope, id) else null
        db.syncQueueDao().upsert(
            SyncQueueEntity(existing?.clientUid ?: UUID.randomUUID().toString(), scope, id, baseUpdatedAt, mapAdapter.toJson(payload), existing?.createdAt ?: System.currentTimeMillis()),
        )
    }

    suspend fun syncNow() {
        pushPendingChanges()
        bootstrap()
        refreshPigeons()
    }

    suspend fun pushPendingChanges() {
        val queue = db.syncQueueDao().getBatch(100)
        if (queue.isEmpty()) return
        fun request(item: SyncQueueEntity) = SyncEntityRequest(item.clientUid, item.entityId?.takeIf { it > 0 }, item.baseUpdatedAt, true, "client_wins", mapAdapter.fromJson(item.payload) ?: emptyMap())
        val done = linkedSetOf<String>()
        val normal = queue.filter { it.scope != "in_progress" }
        if (normal.isNotEmpty()) {
            val response = api.sync(MobileSyncRequest(normal.filter { it.scope == "card" }.map(::request), normal.filter { it.scope == "festival" }.map(::request)))
            if (response.ok) {
                val applied = (response.cards + response.festivals).filter { it.status in setOf("applied", "skipped_server_wins") }
                applied.mapNotNullTo(done) { it.clientUid }
                normal.filter { it.entityId != null && it.entityId < 0 && it.clientUid in done }.forEach { db.cardDao().deleteById(it.entityId!!) }
            }
        }
        val progress = queue.filter { it.scope == "in_progress" }
        if (progress.isNotEmpty()) {
            val response = api.syncInProgress(MobileInProgressSyncRequest(progress.map(::request)))
            if (response.ok) {
                response.items.filter { it.status in setOf("applied", "skipped_server_wins") }.mapNotNullTo(done) { it.clientUid }
                progress.filter { it.entityId != null && it.entityId < 0 && it.clientUid in done }.forEach { db.inProgressDao().deleteById(it.entityId!!) }
            }
        }
        if (done.isNotEmpty()) db.syncQueueDao().deleteByIds(done.toList())
    }

    suspend fun refreshPigeons() {
        val response = api.pigeons()
        if (response.ok) db.pigeonDao().upsertAll(response.messages.map { PigeonMessageEntity(it.id, it.chatUserId, it.chatAlias, it.direction, it.body, it.createdAt, it.isRead, anyAdapter.toJson(it.emojiUrls)) })
    }

    suspend fun sendPigeon(alias: String, body: String) {
        val response = api.sendPigeon(PigeonSendRequest(alias.trim(), body.trim()))
        if (!response.ok) error(response.detail ?: "Не удалось отправить голубя")
        refreshPigeons()
    }

    fun decode(json: String): Map<String, Any?> = mapAdapter.fromJson(json) ?: emptyMap()
}
