package ru.cosplayplanner.mobile.data.local

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.PrimaryKey

@Entity(tableName = "user_profile")
data class UserProfileEntity(
    @PrimaryKey val id: Long,
    val username: String,
    @ColumnInfo(name = "cosplay_nick") val cosplayNick: String?,
    val email: String,
    @ColumnInfo(name = "home_city") val homeCity: String?,
)

@Entity(tableName = "cosplan_cards")
data class CosplanCardEntity(
    @PrimaryKey val id: Long,
    @ColumnInfo(name = "updated_at") val updatedAt: String?,
    @ColumnInfo(name = "payload_json") val payloadJson: String,
)

@Entity(tableName = "festivals")
data class FestivalEntity(
    @PrimaryKey val id: Long,
    @ColumnInfo(name = "updated_at") val updatedAt: String?,
    @ColumnInfo(name = "payload_json") val payloadJson: String,
)

@Entity(tableName = "sync_queue")
data class SyncQueueEntity(
    @PrimaryKey val clientUid: String,
    val scope: String,
    @ColumnInfo(name = "entity_id") val entityId: Long?,
    @ColumnInfo(name = "base_updated_at") val baseUpdatedAt: String?,
    val payload: String,
    @ColumnInfo(name = "created_at") val createdAt: Long,
)

@Entity(tableName = "sync_conflicts")
data class SyncConflictEntity(
    @PrimaryKey val clientUid: String,
    val scope: String,
    @ColumnInfo(name = "entity_id") val entityId: Long?,
    val message: String?,
    @ColumnInfo(name = "conflict_fields_json") val conflictFieldsJson: String,
    @ColumnInfo(name = "server_record_json") val serverRecordJson: String,
    @ColumnInfo(name = "incoming_payload_json") val incomingPayloadJson: String,
    @ColumnInfo(name = "suggested_hybrid_payload_json") val suggestedHybridPayloadJson: String,
    @ColumnInfo(name = "created_at") val createdAt: Long,
    @ColumnInfo(name = "is_resolved") val isResolved: Boolean = false,
)
