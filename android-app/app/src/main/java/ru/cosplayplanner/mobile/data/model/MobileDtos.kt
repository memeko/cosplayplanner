package ru.cosplayplanner.mobile.data.model

import com.squareup.moshi.Json

data class MobileBootstrapResponse(
    val ok: Boolean,
    @Json(name = "server_time") val serverTime: String?,
    val user: MobileUserDto,
    val cards: List<MobileCardDto>,
    val festivals: List<MobileFestivalDto>,
)

data class MobileUserDto(
    val id: Long,
    val username: String,
    @Json(name = "cosplay_nick") val cosplayNick: String?,
    val email: String,
    @Json(name = "home_city") val homeCity: String?,
)

data class MobileCardDto(
    val id: Long,
    @Json(name = "updated_at") val updatedAt: String?,
    val payload: Map<String, Any?>,
)

data class MobileFestivalDto(
    val id: Long,
    @Json(name = "updated_at") val updatedAt: String?,
    val payload: Map<String, Any?>,
)

data class MobileSyncRequest(
    val cards: List<SyncEntityRequest>,
    val festivals: List<SyncEntityRequest>,
)

data class SyncEntityRequest(
    @Json(name = "client_uid") val clientUid: String,
    val id: Long?,
    @Json(name = "base_updated_at") val baseUpdatedAt: String?,
    val force: Boolean,
    val resolution: String,
    val payload: Map<String, Any?>,
)

data class MobileSyncResponse(
    val ok: Boolean,
    @Json(name = "server_time") val serverTime: String?,
    val cards: List<SyncEntityResult>,
    val festivals: List<SyncEntityResult>,
)

data class SyncEntityResult(
    val status: String,
    @Json(name = "client_uid") val clientUid: String?,
    val id: Long?,
    val message: String?,
    @Json(name = "conflict_fields") val conflictFields: List<String>?,
    @Json(name = "server_record") val serverRecord: Map<String, Any?>?,
    @Json(name = "incoming_payload") val incomingPayload: Map<String, Any?>?,
    @Json(name = "suggested_hybrid_payload") val suggestedHybridPayload: Map<String, Any?>?,
)
