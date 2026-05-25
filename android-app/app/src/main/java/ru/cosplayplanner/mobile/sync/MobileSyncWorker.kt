package ru.cosplayplanner.mobile.sync

import android.content.Context
import androidx.work.CoroutineWorker
import androidx.work.WorkerParameters
import androidx.hilt.work.HiltWorker
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import ru.cosplayplanner.mobile.data.SyncRepository

@HiltWorker
class MobileSyncWorker @AssistedInject constructor(
    @Assisted appContext: Context,
    @Assisted workerParams: WorkerParameters,
    private val syncRepository: SyncRepository,
) : CoroutineWorker(appContext, workerParams) {
    override suspend fun doWork(): Result {
        return try {
            // Pull server-side updates (including festivals), then push pending local ops.
            syncRepository.bootstrap()
            syncRepository.pushPendingChanges()
            Result.success()
        } catch (_: Exception) {
            Result.retry()
        }
    }
}
