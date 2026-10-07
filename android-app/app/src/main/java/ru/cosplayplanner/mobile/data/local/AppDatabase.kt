package ru.cosplayplanner.mobile.data.local

import androidx.room.Dao
import androidx.room.Database
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.RoomDatabase
import kotlinx.coroutines.flow.Flow

@Dao
interface UserDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(user: UserProfileEntity)
}

@Dao
interface CardDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(items: List<CosplanCardEntity>)

    @Query("SELECT * FROM cosplan_cards")
    suspend fun getAll(): List<CosplanCardEntity>

    @Query("SELECT * FROM cosplan_cards ORDER BY updated_at DESC")
    fun observeAll(): Flow<List<CosplanCardEntity>>

    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(item: CosplanCardEntity)

    @Query("DELETE FROM cosplan_cards WHERE id = :id")
    suspend fun deleteById(id: Long)
}

@Dao
interface FestivalDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(items: List<FestivalEntity>)

    @Query("SELECT * FROM festivals")
    suspend fun getAll(): List<FestivalEntity>

    @Query("SELECT * FROM festivals WHERE (event_date BETWEEN :today AND :until) OR (is_going = 1 AND (event_date IS NULL OR event_date >= :today)) ORDER BY event_date")
    fun observeOfflineWindow(today: String, until: String): Flow<List<FestivalEntity>>
}

@Dao
interface InProgressDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(items: List<InProgressEntity>)
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(item: InProgressEntity)
    @Query("SELECT * FROM in_progress_cards ORDER BY updated_at DESC")
    fun observeAll(): Flow<List<InProgressEntity>>
    @Query("DELETE FROM in_progress_cards WHERE id = :id")
    suspend fun deleteById(id: Long)
}

@Dao
interface PigeonDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(items: List<PigeonMessageEntity>)
    @Query("SELECT * FROM pigeon_messages ORDER BY created_at DESC")
    fun observeAll(): Flow<List<PigeonMessageEntity>>
}

@Dao
interface SyncQueueDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(item: SyncQueueEntity)

    @Query("SELECT * FROM sync_queue ORDER BY created_at ASC LIMIT :limit")
    suspend fun getBatch(limit: Int = 50): List<SyncQueueEntity>

    @Query("SELECT * FROM sync_queue WHERE scope = :scope AND entity_id = :entityId ORDER BY created_at DESC LIMIT 1")
    suspend fun findForLocalEntity(scope: String, entityId: Long): SyncQueueEntity?

    @Query("DELETE FROM sync_queue WHERE clientUid IN (:ids)")
    suspend fun deleteByIds(ids: List<String>)
}

@Dao
interface SyncConflictDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(item: SyncConflictEntity)

    @Query("SELECT * FROM sync_conflicts WHERE is_resolved = 0 ORDER BY created_at DESC")
    suspend fun getUnresolved(): List<SyncConflictEntity>
}

@Database(
    entities = [
        UserProfileEntity::class,
        CosplanCardEntity::class,
        FestivalEntity::class,
        SyncQueueEntity::class,
        SyncConflictEntity::class,
        InProgressEntity::class,
        PigeonMessageEntity::class,
    ],
    version = 3,
    exportSchema = false,
)
abstract class AppDatabase : RoomDatabase() {
    abstract fun userDao(): UserDao
    abstract fun cardDao(): CardDao
    abstract fun festivalDao(): FestivalDao
    abstract fun syncQueueDao(): SyncQueueDao
    abstract fun syncConflictDao(): SyncConflictDao
    abstract fun inProgressDao(): InProgressDao
    abstract fun pigeonDao(): PigeonDao
}
