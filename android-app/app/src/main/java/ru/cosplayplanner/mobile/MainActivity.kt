package ru.cosplayplanner.mobile

import android.content.pm.ActivityInfo
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.viewModels
import androidx.compose.foundation.layout.*
import androidx.compose.foundation.clickable
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Brush
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.PasswordVisualTransformation
import androidx.compose.ui.unit.dp
import androidx.hilt.navigation.compose.hiltViewModel
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.work.*
import dagger.hilt.android.AndroidEntryPoint
import java.util.concurrent.TimeUnit
import ru.cosplayplanner.mobile.data.local.*
import ru.cosplayplanner.mobile.sync.MobileSyncWorker

private val Violet = Color(0xFF6C63FF)
private val DarkBackground = Color(0xFF111322)
private val DarkSurface = Color(0xFF1B1E32)
private val LightBackground = Color(0xFFF4F8FF)

@AndroidEntryPoint
class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        if (!resources.getBoolean(R.bool.is_tablet)) requestedOrientation = ActivityInfo.SCREEN_ORIENTATION_PORTRAIT
        scheduleBackgroundSync()
        setContent { CosplayPlannerApp() }
    }

    private fun scheduleBackgroundSync() {
        val request = PeriodicWorkRequestBuilder<MobileSyncWorker>(15, TimeUnit.MINUTES)
            .setConstraints(Constraints.Builder().setRequiredNetworkType(NetworkType.CONNECTED).build()).build()
        WorkManager.getInstance(this).enqueueUniquePeriodicWork("mobile_offline_sync", ExistingPeriodicWorkPolicy.KEEP, request)
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun CosplayPlannerApp(vm: MobileViewModel = hiltViewModel()) {
    val state by vm.state.collectAsStateWithLifecycle()
    var dark by remember { mutableStateOf(true) }
    var entered by remember { mutableStateOf(false) }
    val colors = if (dark) darkColorScheme(primary = Color(0xFF8B83FF), background = DarkBackground, surface = DarkSurface)
    else lightColorScheme(primary = Violet, background = LightBackground, surface = Color.White)
    MaterialTheme(colorScheme = colors) {
        Surface(Modifier.fillMaxSize()) {
            if (!entered && !state.signedIn && state.cards.isEmpty() && state.progress.isEmpty()) {
                LoginScreen(state.syncing, state.error, onLogin = vm::login, onOffline = { entered = true })
            } else {
                Workspace(state, dark, onDark = { dark = it }, onSync = vm::sync, onSend = vm::sendPigeon, onCard = vm::updateCard, onProgress = vm::updateProgress)
            }
        }
    }
}

@Composable
private fun LoginScreen(busy: Boolean, error: String?, onLogin: (String, String) -> Unit, onOffline: () -> Unit) {
    var login by remember { mutableStateOf("") }; var password by remember { mutableStateOf("") }
    Column(Modifier.fillMaxSize().padding(28.dp), verticalArrangement = Arrangement.Center) {
        Icon(Icons.Default.AutoAwesome, null, tint = MaterialTheme.colorScheme.primary, modifier = Modifier.size(48.dp))
        Spacer(Modifier.height(18.dp)); Text("Cosplay Planner", style = MaterialTheme.typography.headlineLarge, fontWeight = FontWeight.ExtraBold)
        Text("Планируй. Создавай. Воплощай.", color = MaterialTheme.colorScheme.onSurfaceVariant)
        Spacer(Modifier.height(30.dp))
        OutlinedTextField(login, { login = it }, label = { Text("Логин или почта") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
        Spacer(Modifier.height(12.dp))
        OutlinedTextField(password, { password = it }, label = { Text("Пароль") }, modifier = Modifier.fillMaxWidth(), visualTransformation = PasswordVisualTransformation(), singleLine = true)
        if (error != null) Text(error, color = MaterialTheme.colorScheme.error, modifier = Modifier.padding(top = 10.dp))
        Spacer(Modifier.height(18.dp)); Button({ onLogin(login, password) }, enabled = !busy && login.isNotBlank() && password.isNotBlank(), modifier = Modifier.fillMaxWidth()) { Text(if (busy) "Подключаемся…" else "Войти") }
        TextButton(onOffline, modifier = Modifier.align(Alignment.CenterHorizontally)) { Text("Открыть сохранённые данные офлайн") }
    }
}

private enum class Tab(val title: String) { Plans("Коспланы"), Progress("В процессе"), Festivals("Фестивали"), Pigeons("Голуби") }

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun Workspace(state: MobileUiState, dark: Boolean, onDark: (Boolean) -> Unit, onSync: () -> Unit, onSend: (String, String) -> Unit, onCard: (CosplanCardEntity, Int) -> Unit, onProgress: (InProgressEntity, Boolean) -> Unit) {
    var tab by remember { mutableStateOf(Tab.Plans) }
    Scaffold(
        topBar = { TopAppBar(title = { Column { Text("Cosplay Planner", fontWeight = FontWeight.Bold); Text(if (state.syncing) "Синхронизация…" else "Данные доступны офлайн", style = MaterialTheme.typography.labelSmall) } }, actions = { IconButton(onSync) { Icon(Icons.Default.Sync, "Синхронизировать") }; IconButton({ onDark(!dark) }) { Icon(if (dark) Icons.Default.LightMode else Icons.Default.DarkMode, "Тема") } }) },
        bottomBar = { NavigationBar { Tab.entries.forEach { item -> NavigationBarItem(selected = tab == item, onClick = { tab = item }, icon = { Icon(when(item) { Tab.Plans -> Icons.Default.Style; Tab.Progress -> Icons.Default.Checklist; Tab.Festivals -> Icons.Default.Event; Tab.Pigeons -> Icons.Default.Send }, null) }, label = { Text(item.title) }) } } },
    ) { padding ->
        Box(Modifier.padding(padding).fillMaxSize()) {
            when (tab) {
                Tab.Plans -> PlansScreen(state.cards, onCard)
                Tab.Progress -> ProgressScreen(state.progress, onProgress)
                Tab.Festivals -> FestivalsScreen(state.festivals)
                Tab.Pigeons -> PigeonsScreen(state.pigeons, onSend)
            }
            state.error?.let { AssistChip({}, { Text(it) }, modifier = Modifier.align(Alignment.TopCenter).padding(8.dp), leadingIcon = { Icon(Icons.Default.CloudOff, null) }) }
        }
    }
}

@Composable private fun Empty(text: String) = Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) { Text(text, color = MaterialTheme.colorScheme.onSurfaceVariant) }
@Composable private fun GlassCard(content: @Composable ColumnScope.() -> Unit) = Card(Modifier.fillMaxWidth(), shape = RoundedCornerShape(24.dp), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = .72f))) { Column(Modifier.padding(18.dp), content = content) }

@Composable
private fun PlansScreen(cards: List<CosplanCardEntity>, onUpdate: (CosplanCardEntity, Int) -> Unit) {
    if (cards.isEmpty()) return Empty("Коспланы появятся после первой синхронизации")
    LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { items(cards, key = { it.id }) { card ->
        val data = remember(card.payloadJson) { Regex("\"(character_name|fandom|status_percent)\"\\s*:\\s*(?:\"([^\"]*)\"|([0-9]+))").findAll(card.payloadJson).associate { it.groupValues[1] to (it.groupValues[2].ifBlank { it.groupValues[3] }) } }
        val percent = data["status_percent"]?.toIntOrNull() ?: 0
        GlassCard { Text(data["character_name"] ?: "Без имени", style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.Bold); Text(data["fandom"] ?: "Фандом не указан", color = MaterialTheme.colorScheme.onSurfaceVariant); Spacer(Modifier.height(12.dp)); LinearProgressIndicator({ percent / 100f }, Modifier.fillMaxWidth()); Row(verticalAlignment = Alignment.CenterVertically) { Text("$percent%", modifier = Modifier.weight(1f)); IconButton({ onUpdate(card, (percent + 10).coerceAtMost(100)) }) { Icon(Icons.Default.AddCircle, "Добавить 10%") } } }
    } }
}

@Composable
private fun ProgressScreen(rows: List<InProgressEntity>, onUpdate: (InProgressEntity, Boolean) -> Unit) {
    if (rows.isEmpty()) return Empty("Карточки «В процессе» пока не сохранены")
    LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { items(rows, key = { it.id }) { row ->
        val frozen = row.payloadJson.contains("\"is_frozen\":true")
        GlassCard { Text("Проект #${row.cardId}", style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.Bold); Text(if (frozen) "Приостановлен" else "В активной работе"); Switch(frozen, { onUpdate(row, it) }) }
    } }
}

@Composable
private fun FestivalsScreen(rows: List<FestivalEntity>) {
    if (rows.isEmpty()) return Empty("Нет фестивалей на ближайшие 30 дней")
    LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { item { Text("Ближайшие 30 дней и все отмеченные «Я иду»", color = MaterialTheme.colorScheme.onSurfaceVariant) }; items(rows, key = { it.id }) { row ->
        val name = Regex("\"name\"\\s*:\\s*\"([^\"]*)\"").find(row.payloadJson)?.groupValues?.get(1) ?: "Фестиваль"
        GlassCard { Row(verticalAlignment = Alignment.CenterVertically) { Icon(Icons.Default.AutoAwesome, null, tint = MaterialTheme.colorScheme.primary); Spacer(Modifier.width(12.dp)); Column(Modifier.weight(1f)) { Text(name, fontWeight = FontWeight.Bold); Text(row.eventDate ?: "Дата уточняется") }; if (row.isGoing) AssistChip({}, { Text("Я иду") }) } }
    } }
}

@Composable
private fun PigeonsScreen(messages: List<PigeonMessageEntity>, onSend: (String, String) -> Unit) {
    val dialogs = remember(messages) { messages.groupBy { it.chatUserId }.values.sortedByDescending { chat -> chat.maxOfOrNull { it.id } ?: 0L } }
    var selectedChatId by remember { mutableStateOf<Long?>(null) }
    var creatingDialog by remember { mutableStateOf(false) }

    when {
        creatingDialog -> NewPigeonDialog(
            onBack = { creatingDialog = false },
            onSend = { alias, body -> onSend(alias, body); creatingDialog = false },
        )
        selectedChatId != null -> {
            val chat = dialogs.firstOrNull { it.firstOrNull()?.chatUserId == selectedChatId }
            if (chat == null) selectedChatId = null else PigeonConversation(chat, onBack = { selectedChatId = null }, onSend = onSend)
        }
        else -> Column(Modifier.fillMaxSize()) {
            Row(Modifier.fillMaxWidth().padding(horizontal = 18.dp, vertical = 12.dp), verticalAlignment = Alignment.CenterVertically) {
                Column(Modifier.weight(1f)) { Text("Диалоги", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.ExtraBold); Text("Письма хранятся локально для чтения", color = MaterialTheme.colorScheme.onSurfaceVariant, style = MaterialTheme.typography.bodySmall) }
                FilledIconButton({ creatingDialog = true }) { Icon(Icons.Default.Edit, "Новый диалог") }
            }
            if (dialogs.isEmpty()) Empty("Пока нет диалогов. Отправьте первого голубя ✦") else LazyColumn(Modifier.fillMaxSize().padding(horizontal = 16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
                items(dialogs, key = { it.first().chatUserId }) { chat ->
                    val latest = chat.maxBy { it.id }
                    Card(Modifier.fillMaxWidth().clickable { selectedChatId = latest.chatUserId }, shape = RoundedCornerShape(24.dp), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = .72f))) {
                        Row(Modifier.padding(16.dp), verticalAlignment = Alignment.CenterVertically) {
                            Surface(shape = RoundedCornerShape(18.dp), color = MaterialTheme.colorScheme.primaryContainer, modifier = Modifier.size(52.dp)) { Box(contentAlignment = Alignment.Center) { Icon(Icons.Default.Send, null, tint = MaterialTheme.colorScheme.primary) } }
                            Spacer(Modifier.width(14.dp)); Column(Modifier.weight(1f)) { Text(if (latest.chatUserId == 0L) "Избранное" else "@${latest.chatAlias}", fontWeight = FontWeight.Bold, style = MaterialTheme.typography.titleMedium); Text(latest.body.ifBlank { "Вложение" }, maxLines = 1, color = MaterialTheme.colorScheme.onSurfaceVariant) }
                            Icon(Icons.Default.ChevronRight, null, tint = MaterialTheme.colorScheme.onSurfaceVariant)
                        }
                    }
                }
            }
        }
    }
}

@Composable
private fun NewPigeonDialog(onBack: () -> Unit, onSend: (String, String) -> Unit) {
    var alias by remember { mutableStateOf("") }; var body by remember { mutableStateOf("") }
    Column(Modifier.fillMaxSize().padding(18.dp)) {
        Row(verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "Назад") }; Text("Новый голубь", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.Bold) }
        Spacer(Modifier.height(18.dp)); OutlinedTextField(alias, { alias = it }, label = { Text("Ник получателя") }, placeholder = { Text("@cosplayer") }, singleLine = true, modifier = Modifier.fillMaxWidth())
        Spacer(Modifier.height(12.dp)); OutlinedTextField(body, { body = it }, label = { Text("Сообщение") }, minLines = 5, modifier = Modifier.fillMaxWidth())
        Spacer(Modifier.height(8.dp)); Text("Отправка доступна только при подключении к сети", color = MaterialTheme.colorScheme.onSurfaceVariant, style = MaterialTheme.typography.bodySmall)
        Spacer(Modifier.height(18.dp)); Button({ onSend(alias, body) }, enabled = alias.isNotBlank() && body.isNotBlank(), modifier = Modifier.fillMaxWidth()) { Icon(Icons.Default.Send, null); Spacer(Modifier.width(8.dp)); Text("Отправить голубя") }
    }
}

@Composable
private fun PigeonConversation(messages: List<PigeonMessageEntity>, onBack: () -> Unit, onSend: (String, String) -> Unit) {
    val chat = messages.first(); var body by remember(chat.chatUserId) { mutableStateOf("") }
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.fillMaxWidth().padding(8.dp), verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "К диалогам") }; Column { Text(if (chat.chatUserId == 0L) "Избранное" else "@${chat.chatAlias}", fontWeight = FontWeight.Bold, style = MaterialTheme.typography.titleLarge); Text("Голубиная почта", color = MaterialTheme.colorScheme.onSurfaceVariant, style = MaterialTheme.typography.labelSmall) } }
        HorizontalDivider()
        LazyColumn(Modifier.weight(1f).fillMaxWidth().padding(14.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            items(messages.sortedBy { it.id }, key = { it.id }) { message ->
                val outgoing = message.direction == "out"
                Row(Modifier.fillMaxWidth(), horizontalArrangement = if (outgoing) Arrangement.End else Arrangement.Start) {
                    Surface(shape = RoundedCornerShape(20.dp), color = if (outgoing) MaterialTheme.colorScheme.primaryContainer else MaterialTheme.colorScheme.surfaceVariant, modifier = Modifier.widthIn(max = 300.dp)) {
                        Column(Modifier.padding(horizontal = 16.dp, vertical = 11.dp)) { Text(message.body); message.createdAt?.let { Text(it.take(16).replace('T', ' '), style = MaterialTheme.typography.labelSmall, color = MaterialTheme.colorScheme.onSurfaceVariant) } }
                    }
                }
            }
        }
        Row(Modifier.fillMaxWidth().padding(12.dp), verticalAlignment = Alignment.CenterVertically) { OutlinedTextField(body, { body = it }, placeholder = { Text("Написать сообщение…") }, modifier = Modifier.weight(1f), maxLines = 4); Spacer(Modifier.width(8.dp)); FilledIconButton({ if (body.isNotBlank()) { onSend(chat.chatAlias, body); body = "" } }, enabled = body.isNotBlank()) { Icon(Icons.Default.Send, "Отправить") } }
    }
}
