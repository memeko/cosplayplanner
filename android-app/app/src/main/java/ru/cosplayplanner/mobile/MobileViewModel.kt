package ru.cosplayplanner.mobile

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import dagger.hilt.android.lifecycle.HiltViewModel
import javax.inject.Inject
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import ru.cosplayplanner.mobile.data.SyncRepository
import ru.cosplayplanner.mobile.data.local.*

data class MobileUiState(
    val cards: List<CosplanCardEntity> = emptyList(),
    val progress: List<InProgressEntity> = emptyList(),
    val festivals: List<FestivalEntity> = emptyList(),
    val pigeons: List<PigeonMessageEntity> = emptyList(),
    val calendarEvents: List<CalendarEventEntity> = emptyList(),
    val contentPosts: List<ContentPostEntity> = emptyList(),
    val homeCity: String? = null,
    val signedIn: Boolean = false,
    val syncing: Boolean = false,
    val error: String? = null,
)

@HiltViewModel
class MobileViewModel @Inject constructor(private val repository: SyncRepository) : ViewModel() {
    private val session = MutableStateFlow(false)
    private val busy = MutableStateFlow(false)
    private val error = MutableStateFlow<String?>(null)
    val state: StateFlow<MobileUiState> = combine(
        repository.cards(), repository.inProgress(), repository.festivals(), repository.pigeons(), repository.calendarEvents(), repository.contentPosts(), repository.homeCity,
        session, busy, error,
    ) { values ->
        @Suppress("UNCHECKED_CAST")
        MobileUiState(
            cards = values[0] as List<CosplanCardEntity>,
            progress = values[1] as List<InProgressEntity>,
            festivals = values[2] as List<FestivalEntity>,
            pigeons = values[3] as List<PigeonMessageEntity>,
            calendarEvents = values[4] as List<CalendarEventEntity>, contentPosts = values[5] as List<ContentPostEntity>, homeCity = values[6] as String?,
            signedIn = values[7] as Boolean, syncing = values[8] as Boolean, error = values[9] as String?,
        )
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), MobileUiState())

    fun login(login: String, password: String) = launchNetwork {
        val result = repository.login(login, password)
        if (!result.ok) error(result.detail ?: "Ошибка входа")
        session.value = true
        repository.syncNow()
    }
    fun sync() = launchNetwork { repository.syncNow(); session.value = true }
    fun sendPigeon(alias: String, text: String) = launchNetwork { repository.sendPigeon(alias, text) }
    fun saveCard(item: CosplanCardEntity?, payload: Map<String, Any?>) = viewModelScope.launch {
        if (item == null) repository.createCard(payload) else repository.editCard(item, payload)
    }
    fun saveProgress(item: InProgressEntity, payload: Map<String, Any?>) = viewModelScope.launch { repository.editInProgress(item, payload) }
    fun createProgress(cardId: Long) = viewModelScope.launch { repository.createInProgress(cardId) }
    fun clearError() { error.value = null }
    private fun launchNetwork(block: suspend () -> Unit) = viewModelScope.launch {
        busy.value = true; error.value = null
        runCatching { block() }.onFailure { error.value = it.message ?: "Нет соединения с сервером" }
        busy.value = false
    }
}
