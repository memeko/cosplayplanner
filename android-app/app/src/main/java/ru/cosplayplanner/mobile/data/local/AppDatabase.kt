package ru.cosplayplanner.mobile.data.local

import androidx.room.Dao
import androidx.room.Database
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import androidx.room.RoomDatabase

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
}

@Dao
interface FestivalDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsertAll(items: List<FestivalEntity>)

    @Query("SELECT * FROM festivals")
    suspend fun getAll(): List<FestivalEntity>
}

@Dao
interface SyncQueueDao {
    @Insert(onConflict = OnConflictStrategy.REPLACE)
    suspend fun upsert(item: SyncQueueEntity)

    @Query("SELECT * FROM sync_queue ORDER BY created_at ASC LIMIT :limit")
    suspend fun getBatch(limit: Int = 50): List<SyncQueueEntity>

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
    ],
    version = 2,
    exportSchema = false,
)
abstract class AppDatabase : RoomDatabase() {
    abstract fun userDao(): UserDao
    abstract fun cardDao(): CardDao
    abstract fun festivalDao(): FestivalDao
    abstract fun syncQueueDao(): SyncQueueDao
    abstract fun syncConflictDao(): SyncConflictDao
}
