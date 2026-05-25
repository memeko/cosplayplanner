package ru.cosplayplanner.mobile.data

import com.squareup.moshi.Types
import com.squareup.moshi.Moshi
import javax.inject.Inject
import javax.inject.Singleton
import ru.cosplayplanner.mobile.data.local.AppDatabase
import ru.cosplayplanner.mobile.data.local.CosplanCardEntity
import ru.cosplayplanner.mobile.data.local.FestivalEntity
import ru.cosplayplanner.mobile.data.local.SyncConflictEntity
import ru.cosplayplanner.mobile.data.local.SyncQueueEntity
import ru.cosplayplanner.mobile.data.local.UserProfileEntity
import ru.cosplayplanner.mobile.data.model.MobileSyncRequest
import ru.cosplayplanner.mobile.data.model.SyncEntityResult
import ru.cosplayplanner.mobile.data.model.SyncEntityRequest
import ru.cosplayplanner.mobile.data.remote.MobileApi

@Singleton
class SyncRepository @Inject constructor(
    private val api: MobileApi,
    private val db: AppDatabase,
    private val moshi: Moshi,
) {
    private val rawMapAdapter = moshi.adapter(Map::class.java)
    private val rawAnyAdapter = moshi.adapter(Any::class.java).serializeNulls()
    private val typedMapAdapter = moshi.adapter<Map<String, Any?>>(
        Types.newParameterizedType(
            Map::class.java,
            String::class.java,
            Any::class.java,
        ),
    )

    suspend fun bootstrap(since: String? = null) {
        val response = api.bootstrap(since)
        if (!response.ok) return

        db.userDao().upsert(
            UserProfileEntity(
                id = response.user.id,
                username = response.user.username,
                cosplayNick = response.user.cosplayNick,
                email = response.user.email,
                homeCity = response.user.homeCity,
            ),
        )

        db.cardDao().upsertAll(
            response.cards.map {
                CosplanCardEntity(
                    id = it.id,
                    updatedAt = it.updatedAt,
                    payloadJson = typedMapAdapter.toJson(it.payload),
                )
            },
        )

        db.festivalDao().upsertAll(
            response.festivals.map {
                FestivalEntity(
                    id = it.id,
                    updatedAt = it.updatedAt,
                    payloadJson = typedMapAdapter.toJson(it.payload),
                )
            },
        )
    }

    suspend fun pushPendingChanges() {
        val queue = db.syncQueueDao().getBatch(limit = 100)
        if (queue.isEmpty()) return

        val cardOps = mutableListOf<SyncEntityRequest>()
        val festivalOps = mutableListOf<SyncEntityRequest>()

        for (item in queue) {
            val payload = rawMapAdapter.fromJson(item.payload) as? Map<String, Any?> ?: emptyMap()
            val request = SyncEntityRequest(
                clientUid = item.clientUid,
                id = item.entityId,
                baseUpdatedAt = item.baseUpdatedAt,
                force = false,
                resolution = "client_wins",
                payload = payload,
            )
            if (item.scope == "card") {
                cardOps += request
            } else if (item.scope == "festival") {
                festivalOps += request
            }
        }

        val response = api.sync(
            MobileSyncRequest(
                cards = cardOps,
                festivals = festivalOps,
            ),
        )
        if (!response.ok) return

        val queueIdsToDelete = linkedSetOf<String>()
        var shouldRefreshLocalSnapshot = false

        shouldRefreshLocalSnapshot = handleSyncResults(
            scope = "card",
            results = response.cards,
            queueIdsToDelete = queueIdsToDelete,
        ) || shouldRefreshLocalSnapshot
        shouldRefreshLocalSnapshot = handleSyncResults(
            scope = "festival",
            results = response.festivals,
            queueIdsToDelete = queueIdsToDelete,
        ) || shouldRefreshLocalSnapshot

        if (queueIdsToDelete.isNotEmpty()) {
            db.syncQueueDao().deleteByIds(queueIdsToDelete.toList())
        }
        if (shouldRefreshLocalSnapshot) {
            // Pull the latest server version so festival and card snapshots stay aligned.
            bootstrap()
        }
    }

    private suspend fun handleSyncResults(
        scope: String,
        results: List<SyncEntityResult>,
        queueIdsToDelete: MutableSet<String>,
    ): Boolean {
        var shouldRefresh = false
        for (result in results) {
            val clientUid = result.clientUid ?: continue
            when (result.status) {
                "applied" -> {
                    queueIdsToDelete += clientUid
                    shouldRefresh = true
                }

                "conflict" -> {
                    db.syncConflictDao().upsert(
                        SyncConflictEntity(
                            clientUid = clientUid,
                            scope = scope,
                            entityId = result.id,
                            message = result.message,
                            conflictFieldsJson = rawAnyAdapter.toJson(result.conflictFields ?: emptyList<String>()),
                            serverRecordJson = rawAnyAdapter.toJson(result.serverRecord ?: emptyMap<String, Any?>()),
                            incomingPayloadJson = rawAnyAdapter.toJson(result.incomingPayload ?: emptyMap<String, Any?>()),
                            suggestedHybridPayloadJson = rawAnyAdapter.toJson(
                                result.suggestedHybridPayload ?: emptyMap<String, Any?>(),
                            ),
                            createdAt = System.currentTimeMillis(),
                        ),
                    )
                    queueIdsToDelete += clientUid
                    shouldRefresh = true
                }

                "skipped_server_wins" -> {
                    queueIdsToDelete += clientUid
                    shouldRefresh = true
                }
            }
        }
        return shouldRefresh
    }

    suspend fun enqueueLocalCardChange(
        clientUid: String,
        entityId: Long?,
        baseUpdatedAt: String?,
        payload: String,
    ) {
        db.syncQueueDao().upsert(
            SyncQueueEntity(
                clientUid = clientUid,
                scope = "card",
                entityId = entityId,
                baseUpdatedAt = baseUpdatedAt,
                payload = payload,
                createdAt = System.currentTimeMillis(),
            ),
        )
    }

    suspend fun enqueueLocalFestivalChange(
        clientUid: String,
        entityId: Long?,
        baseUpdatedAt: String?,
        payload: String,
    ) {
        db.syncQueueDao().upsert(
            SyncQueueEntity(
                clientUid = clientUid,
                scope = "festival",
                entityId = entityId,
                baseUpdatedAt = baseUpdatedAt,
                payload = payload,
                createdAt = System.currentTimeMillis(),
            ),
        )
    }
}
