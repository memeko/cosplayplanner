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
import org.json.JSONArray
import org.json.JSONObject
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
                Workspace(state, dark, onDark = { dark = it }, onSync = vm::sync, onSend = vm::sendPigeon, onSaveCard = vm::saveCard, onSaveProgress = vm::saveProgress, onCreateProgress = vm::createProgress)
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
private fun Workspace(state: MobileUiState, dark: Boolean, onDark: (Boolean) -> Unit, onSync: () -> Unit, onSend: (String, String) -> Unit, onSaveCard: (CosplanCardEntity?, Map<String, Any?>) -> Unit, onSaveProgress: (InProgressEntity, Map<String, Any?>) -> Unit, onCreateProgress: (Long) -> Unit) {
    var tab by remember { mutableStateOf(Tab.Plans) }
    Scaffold(
        topBar = { TopAppBar(title = { Column { Text("Cosplay Planner", fontWeight = FontWeight.Bold); Text(if (state.syncing) "Синхронизация…" else "Данные доступны офлайн", style = MaterialTheme.typography.labelSmall) } }, actions = { IconButton(onSync) { Icon(Icons.Default.Sync, "Синхронизировать") }; IconButton({ onDark(!dark) }) { Icon(if (dark) Icons.Default.LightMode else Icons.Default.DarkMode, "Тема") } }) },
        bottomBar = { NavigationBar { Tab.entries.forEach { item -> NavigationBarItem(selected = tab == item, onClick = { tab = item }, icon = { Icon(when(item) { Tab.Plans -> Icons.Default.Style; Tab.Progress -> Icons.Default.Checklist; Tab.Festivals -> Icons.Default.Event; Tab.Pigeons -> Icons.Default.Send }, null) }, label = { Text(item.title) }) } } },
    ) { padding ->
        Box(Modifier.padding(padding).fillMaxSize()) {
            when (tab) {
                Tab.Plans -> PlansScreen(state.cards, onSaveCard)
                Tab.Progress -> ProgressScreen(state.progress, state.cards, onSaveProgress, onCreateProgress)
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
private fun PlansScreen(cards: List<CosplanCardEntity>, onSave: (CosplanCardEntity?, Map<String, Any?>) -> Unit) {
    var selected by remember { mutableStateOf<CosplanCardEntity?>(null) }; var creating by remember { mutableStateOf(false) }
    if (selected != null || creating) return CardEditor(selected, onBack = { selected = null; creating = false }, onSave = { card, data -> onSave(card, data); selected = null; creating = false })
    Box(Modifier.fillMaxSize()) {
        if (cards.isEmpty()) Empty("Создайте первый косплан") else LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { items(cards, key = { it.id }) { card ->
            val data = remember(card.payloadJson) { JSONObject(card.payloadJson) }; val percent = data.optInt("status_percent", 0)
            Card(Modifier.fillMaxWidth().clickable { selected = card }, shape = RoundedCornerShape(24.dp), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = .72f))) { Column(Modifier.padding(18.dp)) { Row { Column(Modifier.weight(1f)) { Text(data.text("character_name") ?: "Без имени", style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.Bold); Text(data.text("fandom") ?: "Фандом не указан", color = MaterialTheme.colorScheme.onSurfaceVariant) }; Icon(Icons.Default.ChevronRight, null) }; Spacer(Modifier.height(12.dp)); LinearProgressIndicator({ percent / 100f }, Modifier.fillMaxWidth()); Text("Готовность: $percent%", style = MaterialTheme.typography.labelMedium) } }
        } }
        FloatingActionButton({ creating = true }, modifier = Modifier.align(Alignment.BottomEnd).padding(20.dp)) { Icon(Icons.Default.Add, "Создать косплан") }
    }
}

@Composable
private fun ProgressScreen(rows: List<InProgressEntity>, cards: List<CosplanCardEntity>, onSave: (InProgressEntity, Map<String, Any?>) -> Unit, onCreate: (Long) -> Unit) {
    var selected by remember { mutableStateOf<InProgressEntity?>(null) }; var choosing by remember { mutableStateOf(false) }
    val cardsById = remember(cards) { cards.associateBy { it.id } }
    selected?.let { row -> return ProgressEditor(row, cardTitle(cardsById[row.cardId]), onBack = { selected = null }, onSave = { data -> onSave(row, data); selected = null }) }
    if (choosing) return SelectProgressCard(cards.filter { card -> rows.none { it.cardId == card.id } }, onBack = { choosing = false }, onSelect = { onCreate(it.id); choosing = false })
    Box(Modifier.fillMaxSize()) {
        if (rows.isEmpty()) Empty("Добавьте косплан в работу") else LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { items(rows, key = { it.id }) { row ->
            val frozen = JSONObject(row.payloadJson).optBoolean("is_frozen", false)
            Card(Modifier.fillMaxWidth().clickable { selected = row }, shape = RoundedCornerShape(24.dp), colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant.copy(alpha = .72f))) { Row(Modifier.padding(18.dp), verticalAlignment = Alignment.CenterVertically) { Column(Modifier.weight(1f)) { Text(cardTitle(cardsById[row.cardId]), style = MaterialTheme.typography.titleMedium, fontWeight = FontWeight.Bold); Text(if (frozen) "Приостановлен" else "Активен", color = MaterialTheme.colorScheme.onSurfaceVariant) }; Icon(Icons.Default.ChevronRight, null) } }
        } }
        FloatingActionButton({ choosing = true }, modifier = Modifier.align(Alignment.BottomEnd).padding(20.dp)) { Icon(Icons.Default.Add, "Добавить в процессы") }
    }
}

private fun cardTitle(card: CosplanCardEntity?): String {
    if (card == null) return "Косплан"
    return runCatching { JSONObject(card.payloadJson).text("character_name") ?: "Без имени" }.getOrDefault("Без имени")
}

private fun JSONObject.text(key: String): String? = optString(key, "").trim().takeUnless { it.isBlank() || it == "null" }

@Composable
private fun CardEditor(card: CosplanCardEntity?, onBack: () -> Unit, onSave: (CosplanCardEntity?, Map<String, Any?>) -> Unit) {
    val source = remember(card?.payloadJson) { card?.let { JSONObject(it.payloadJson) } }
    var character by remember(card?.id) { mutableStateOf(source?.text("character_name") ?: "") }
    var fandom by remember(card?.id) { mutableStateOf(source?.text("fandom") ?: "") }
    var city by remember(card?.id) { mutableStateOf(source?.text("city") ?: "") }
    var notes by remember(card?.id) { mutableStateOf(source?.text("notes") ?: "") }
    var percent by remember(card?.id) { mutableFloatStateOf((source?.optInt("status_percent") ?: 0).toFloat()) }
    var priority by remember(card?.id) { mutableStateOf(source?.optBoolean("is_priority") ?: false) }
    var completed by remember(card?.id) { mutableStateOf(source?.optBoolean("is_completed") ?: false) }
    var project by remember(card?.id) { mutableStateOf(source?.optString("plan_type") == "project") }
    LazyColumn(Modifier.fillMaxSize().padding(18.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) {
        item { Row(verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "Назад") }; Text(if (card == null) "Новый косплан" else "Карточка косплана", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.ExtraBold) } }
        item { OutlinedTextField(character, { character = it }, label = { Text("Персонаж") }, modifier = Modifier.fillMaxWidth(), singleLine = true) }
        item { OutlinedTextField(fandom, { fandom = it }, label = { Text("Фандом") }, modifier = Modifier.fillMaxWidth(), singleLine = true) }
        item { OutlinedTextField(city, { city = it }, label = { Text("Город") }, modifier = Modifier.fillMaxWidth(), singleLine = true) }
        item { Text("Готовность: ${percent.toInt()}%", fontWeight = FontWeight.Bold); Slider(percent, { percent = it }, valueRange = 0f..100f, steps = 19) }
        item { GlassCard { SettingSwitch("Проектный косплан", project) { project = it }; SettingSwitch("Приоритет", priority) { priority = it }; SettingSwitch("Завершён", completed) { completed = it } } }
        item { OutlinedTextField(notes, { notes = it }, label = { Text("Заметки") }, modifier = Modifier.fillMaxWidth(), minLines = 4) }
        item { Button({ onSave(card, mapOf("character_name" to character.trim(), "fandom" to fandom.trim().ifBlank { null }, "city" to city.trim().ifBlank { null }, "notes" to notes.trim().ifBlank { null }, "status_percent" to percent.toInt(), "plan_type" to if (project) "project" else "personal", "is_priority" to priority, "is_completed" to completed)) }, enabled = character.isNotBlank(), modifier = Modifier.fillMaxWidth()) { Icon(Icons.Default.Save, null); Spacer(Modifier.width(8.dp)); Text("Сохранить карточку") } }
        item { Spacer(Modifier.height(40.dp)) }
    }
}

@Composable
private fun SettingSwitch(label: String, checked: Boolean, onChange: (Boolean) -> Unit) {
    Row(Modifier.fillMaxWidth(), verticalAlignment = Alignment.CenterVertically) { Text(label, modifier = Modifier.weight(1f)); Switch(checked, onChange) }
}

@Composable
private fun SelectProgressCard(cards: List<CosplanCardEntity>, onBack: () -> Unit, onSelect: (CosplanCardEntity) -> Unit) {
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.padding(8.dp), verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "Назад") }; Text("Добавить в процессы", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.Bold) }
        if (cards.isEmpty()) Empty("Все коспланы уже находятся в работе") else LazyColumn(Modifier.padding(16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) { items(cards, key = { it.id }) { card -> Card(Modifier.fillMaxWidth().clickable { onSelect(card) }, shape = RoundedCornerShape(22.dp)) { Row(Modifier.padding(18.dp), verticalAlignment = Alignment.CenterVertically) { Text(cardTitle(card), modifier = Modifier.weight(1f), fontWeight = FontWeight.Bold); Icon(Icons.Default.AddCircle, null, tint = MaterialTheme.colorScheme.primary) } } } }
    }
}

@Composable
private fun ProgressEditor(row: InProgressEntity, title: String, onBack: () -> Unit, onSave: (Map<String, Any?>) -> Unit) {
    val source = remember(row.payloadJson) { JSONObject(row.payloadJson) }
    var active by remember(row.id) { mutableStateOf(!source.optBoolean("is_frozen", false)) }
    val checklist = remember(row.id) {
        mutableStateListOf<Pair<String, Boolean>>().apply {
            val values = source.optJSONArray("checklist_json") ?: JSONArray()
            for (i in 0 until values.length()) { val item = values.optJSONObject(i); if (item != null) add(item.optString("text") to item.optBoolean("done")) }
        }
    }
    var newTask by remember { mutableStateOf("") }
    Column(Modifier.fillMaxSize()) {
        Row(Modifier.padding(8.dp), verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "Назад") }; Column(Modifier.weight(1f)) { Text(title, style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.ExtraBold); Text("Карточка прогресса", color = MaterialTheme.colorScheme.onSurfaceVariant) } }
        LazyColumn(Modifier.weight(1f).padding(horizontal = 18.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
            item { GlassCard { SettingSwitch("Активен", active) { active = it }; Text(if (active) "Проект отображается среди активных" else "Проект приостановлен", color = MaterialTheme.colorScheme.onSurfaceVariant, style = MaterialTheme.typography.bodySmall) } }
            item { Text("Чек-лист", style = MaterialTheme.typography.titleLarge, fontWeight = FontWeight.Bold) }
            items(checklist.size) { index -> val item = checklist[index]; Card(Modifier.fillMaxWidth(), shape = RoundedCornerShape(18.dp)) { Row(Modifier.padding(10.dp), verticalAlignment = Alignment.CenterVertically) { Checkbox(item.second, { checklist[index] = item.first to it }); Text(item.first, modifier = Modifier.weight(1f)); IconButton({ checklist.removeAt(index) }) { Icon(Icons.Default.DeleteOutline, "Удалить") } } } }
            item { Row(verticalAlignment = Alignment.CenterVertically) { OutlinedTextField(newTask, { newTask = it }, label = { Text("Новая задача") }, modifier = Modifier.weight(1f)); IconButton({ if (newTask.isNotBlank()) { checklist += newTask.trim() to false; newTask = "" } }) { Icon(Icons.Default.AddCircle, "Добавить") } } }
            item { Button({ onSave(mapOf("is_frozen" to !active, "checklist_json" to checklist.map { mapOf("text" to it.first, "done" to it.second) })) }, modifier = Modifier.fillMaxWidth()) { Icon(Icons.Default.Save, null); Spacer(Modifier.width(8.dp)); Text("Сохранить прогресс") } }
            item { Spacer(Modifier.height(30.dp)) }
        }
    }
}

@Composable
private fun FestivalsScreen(rows: List<FestivalEntity>) {
    if (rows.isEmpty()) return Empty("Нет фестивалей на ближайшие 30 дней")
    LazyColumn(Modifier.fillMaxSize().padding(16.dp), verticalArrangement = Arrangement.spacedBy(12.dp)) { item { Text("Ближайшие 30 дней и все отмеченные «Я иду»", color = MaterialTheme.colorScheme.onSurfaceVariant) }; items(rows, key = { it.id }) { row ->
        val data = JSONObject(row.payloadJson); val name = data.text("name") ?: "Фестиваль"; val city = data.text("city")
        GlassCard { Row(verticalAlignment = Alignment.CenterVertically) { Icon(Icons.Default.AutoAwesome, null, tint = MaterialTheme.colorScheme.primary); Spacer(Modifier.width(12.dp)); Column(Modifier.weight(1f)) { Text(name, fontWeight = FontWeight.Bold); Text(listOfNotNull(city, row.eventDate ?: "Дата уточняется").joinToString(" · ")) }; if (row.isGoing) AssistChip({}, { Text("Я иду") }) } }
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
