package ru.cosplayplanner.mobile

import androidx.compose.foundation.layout.*
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.ArrowBack
import androidx.compose.material.icons.filled.ExpandLess
import androidx.compose.material.icons.filled.ExpandMore
import androidx.compose.material.icons.filled.Save
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import org.json.JSONArray
import org.json.JSONObject
import ru.cosplayplanner.mobile.data.local.CosplanCardEntity

private enum class FullFieldKind { Text, Multiline, Boolean, Number, Integer, Date, Lines, Json, Choice }
private data class FullField(val key: String, val label: String, val kind: FullFieldKind = FullFieldKind.Text, val options: List<Pair<String, String>> = emptyList())
private data class FullSection(val title: String, val fields: List<FullField>)

private val fullCardSections = listOf(
    FullSection("Основное", listOf(
        FullField("character_name", "Персонаж / название проекта"), FullField("fandom", "Фандом"), FullField("status_percent", "Готовность, %", FullFieldKind.Integer),
        FullField("is_au", "AU", FullFieldKind.Boolean), FullField("au_text", "Описание AU", FullFieldKind.Multiline),
        FullField("plan_type", "Тип косплана", FullFieldKind.Choice, listOf("personal" to "Личный", "project" to "Проектный")),
        FullField("is_priority", "Приоритет", FullFieldKind.Boolean), FullField("is_completed", "Завершён", FullFieldKind.Boolean), FullField("notes", "Общие заметки", FullFieldKind.Multiline),
    )),
    FullSection("Костюм", listOf(
        FullField("costume_type", "Способ", FullFieldKind.Choice, listOf("sew" to "Пошив", "buy" to "Покупка")),
        FullField("sewing_type", "Кто шьёт", FullFieldKind.Choice, listOf("self" to "Самостоятельно", "outsourced" to "На заказ")),
        FullField("sewing_fabric", "Ткань выбрана", FullFieldKind.Boolean), FullField("sewing_hardware", "Фурнитура выбрана", FullFieldKind.Boolean),
        FullField("sewing_pattern", "Выкройка готова", FullFieldKind.Boolean), FullField("sewing_mockup", "Макет готов", FullFieldKind.Boolean),
        FullField("sewing_fitting", "Примерка проведена", FullFieldKind.Boolean), FullField("sewing_details", "Детали готовы", FullFieldKind.Boolean),
        FullField("costume_executor", "Исполнитель"), FullField("costume_deadline", "Дедлайн", FullFieldKind.Date),
        FullField("costume_prepayment", "Предоплата", FullFieldKind.Number), FullField("costume_postpayment", "Доплата", FullFieldKind.Number),
        FullField("costume_fabric_price", "Стоимость ткани", FullFieldKind.Number), FullField("costume_hardware_price", "Стоимость фурнитуры", FullFieldKind.Number),
        FullField("costume_fabric_rows_json", "Таблица тканей (JSON)", FullFieldKind.Json), FullField("costume_hardware_rows_json", "Таблица фурнитуры (JSON)", FullFieldKind.Json),
        FullField("costume_bought", "Костюм куплен", FullFieldKind.Boolean), FullField("costume_link", "Ссылка на костюм"),
        FullField("costume_buy_price", "Цена покупки", FullFieldKind.Number), FullField("costume_currency", "Валюта"),
        FullField("costume_parts_json", "Части костюма (JSON)", FullFieldKind.Json), FullField("costume_notes", "Заметки по костюму", FullFieldKind.Multiline),
    )),
    FullSection("Обувь", listOf(
        FullField("shoes_type", "Способ", FullFieldKind.Choice, listOf("buy" to "Покупка", "craft" to "Изготовление")), FullField("shoes_bought", "Обувь куплена", FullFieldKind.Boolean),
        FullField("shoes_link", "Ссылка"), FullField("shoes_buy_price", "Цена покупки", FullFieldKind.Number), FullField("shoes_executor", "Исполнитель"),
        FullField("shoes_deadline", "Дедлайн", FullFieldKind.Date), FullField("shoes_price", "Цена изготовления", FullFieldKind.Number), FullField("shoes_currency", "Валюта"),
    )),
    FullSection("Линзы", listOf(
        FullField("lenses_enabled", "Линзы нужны", FullFieldKind.Boolean), FullField("lenses_color", "Цвет"), FullField("lenses_comment", "Комментарий", FullFieldKind.Multiline),
        FullField("lenses_price", "Цена", FullFieldKind.Number), FullField("lenses_currency", "Валюта"),
    )),
    FullSection("Парик", listOf(
        FullField("wig_type", "Способ", FullFieldKind.Choice, listOf("wigmaker" to "Вигмейкер", "buy" to "Покупка", "no_buy" to "Уже есть")),
        FullField("wigmaker_name", "Вигмейкер"), FullField("wig_price", "Цена работы", FullFieldKind.Number), FullField("wig_buy_price", "Цена покупки", FullFieldKind.Number),
        FullField("wig_currency", "Валюта"), FullField("wig_deadline", "Дедлайн", FullFieldKind.Date), FullField("wig_link", "Ссылка"),
        FullField("wig_no_buy_from", "Откуда парик"), FullField("wig_restyle", "Нужен рестайлинг", FullFieldKind.Boolean),
    )),
    FullSection("Крафт", listOf(
        FullField("craft_type", "Способ", FullFieldKind.Choice, listOf("self" to "Самостоятельно", "order" to "На заказ")), FullField("craft_master", "Мастер"),
        FullField("craft_price", "Цена работы", FullFieldKind.Number), FullField("craft_material_price", "Цена материалов", FullFieldKind.Number),
        FullField("craft_deadline", "Дедлайн", FullFieldKind.Date), FullField("craft_currency", "Валюта"), FullField("craft_parts_json", "Части крафта (JSON)", FullFieldKind.Json),
    )),
    FullSection("Проект и команда", listOf(
        FullField("project_leader", "Руководитель проекта"), FullField("cosbands_json", "Коспбенды", FullFieldKind.Lines), FullField("project_deadline", "Дедлайн проекта", FullFieldKind.Date),
        FullField("related_cards_json", "Связанные карточки (JSON)", FullFieldKind.Json), FullField("project_characters_json", "Персонажи проекта (JSON)", FullFieldKind.Json),
        FullField("coproplayers_json", "Соигроки (JSON)", FullFieldKind.Json), FullField("coproplayer_nicks_json", "Ники соигроков", FullFieldKind.Lines),
    )),
    FullSection("Фестивали и заявки", listOf(
        FullField("planned_festivals_json", "Запланированные фестивали", FullFieldKind.Lines), FullField("submission_date", "Дата подачи", FullFieldKind.Date),
        FullField("nominations_json", "Номинации", FullFieldKind.Lines), FullField("city", "Город"),
    )),
    FullSection("Выступление", listOf(
        FullField("performance_track", "Трек / ссылка"), FullField("performance_video_bg_url", "Видео-фон"), FullField("performance_script", "Сценарий", FullFieldKind.Multiline),
        FullField("performance_light_script", "Световой сценарий", FullFieldKind.Multiline), FullField("performance_duration", "Длительность"),
        FullField("performance_plan_json", "План дефиле (JSON)", FullFieldKind.Json), FullField("performance_rehearsal_point", "Место репетиции"),
        FullField("performance_rehearsal_price", "Цена репетиции", FullFieldKind.Number), FullField("performance_rehearsal_currency", "Валюта"),
        FullField("performance_rehearsal_count", "Количество репетиций", FullFieldKind.Integer),
    )),
    FullSection("Фотосет", listOf(
        FullField("photographers_json", "Фотографы", FullFieldKind.Lines), FullField("studios_json", "Студии", FullFieldKind.Lines), FullField("photoset_date", "Дата фотосета", FullFieldKind.Date),
        FullField("photoset_price", "Общая стоимость", FullFieldKind.Number), FullField("photoset_photographer_price", "Фотограф", FullFieldKind.Number),
        FullField("photoset_studio_price", "Студия", FullFieldKind.Number), FullField("photoset_props_price", "Реквизит", FullFieldKind.Number),
        FullField("photoset_extra_price", "Прочие расходы", FullFieldKind.Number), FullField("photoset_currency", "Валюта"),
        FullField("photoset_comment", "Комментарий", FullFieldKind.Multiline), FullField("photoset_props_checklist_json", "Чек-лист реквизита (JSON)", FullFieldKind.Json),
        FullField("photoset_storyboard_rows_json", "Раскадровка (JSON)", FullFieldKind.Json),
    )),
    FullSection("Референсы и расходы", listOf(
        FullField("references_json", "Референсы", FullFieldKind.Lines), FullField("pose_references_json", "Референсы поз", FullFieldKind.Lines),
        FullField("unknown_prices_json", "Неизвестные цены", FullFieldKind.Lines),
    )),
)

@Composable
fun FullCardEditor(card: CosplanCardEntity?, onBack: () -> Unit, onSave: (CosplanCardEntity?, Map<String, Any?>) -> Unit) {
    val original = remember(card?.payloadJson) { jsonObjectToMap(card?.payloadJson?.let(::JSONObject) ?: JSONObject()) }
    val values = remember(card?.id) { mutableStateMapOf<String, Any?>().apply { putAll(original) } }
    val expanded = remember { mutableStateMapOf("Основное" to true) }
    var validationError by remember { mutableStateOf<String?>(null) }

    LazyColumn(Modifier.fillMaxSize().padding(horizontal = 16.dp), verticalArrangement = Arrangement.spacedBy(10.dp)) {
        item { Row(Modifier.padding(vertical = 8.dp), verticalAlignment = Alignment.CenterVertically) { IconButton(onBack) { Icon(Icons.Default.ArrowBack, "Назад") }; Column { Text(if (card == null) "Новый косплан" else "Полная карточка", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.ExtraBold); Text("Все поля синхронизируются с сайтом", color = MaterialTheme.colorScheme.onSurfaceVariant) } } }
        validationError?.let { message -> item { Text(message, color = MaterialTheme.colorScheme.error) } }
        fullCardSections.forEach { section ->
            item(key = "head-${section.title}") { FilledTonalButton({ expanded[section.title] = expanded[section.title] != true }, modifier = Modifier.fillMaxWidth()) { Text(section.title, modifier = Modifier.weight(1f)); Icon(if (expanded[section.title] == true) Icons.Default.ExpandLess else Icons.Default.ExpandMore, null) } }
            if (expanded[section.title] == true) items(section.fields.filter { fieldVisible(it.key, values) }, key = { it.key }) { field -> FullFieldEditor(field, values) }
        }
        item {
            Button({
                val name = values["character_name"]?.toString()?.trim().orEmpty()
                if (name.isBlank()) validationError = "Укажите персонажа или название проекта."
                else {
                    validationError = null
                    onSave(card, values.toMap().toMutableMap().apply { this["character_name"] = name; this["status_percent"] = ((this["status_percent"] as? Number)?.toInt() ?: this["status_percent"]?.toString()?.toIntOrNull() ?: 0).coerceIn(0, 100) })
                }
            }, modifier = Modifier.fillMaxWidth()) { Icon(Icons.Default.Save, null); Spacer(Modifier.width(8.dp)); Text("Сохранить полную карточку") }
        }
        item { Spacer(Modifier.height(48.dp)) }
    }
}

@Composable
private fun FullFieldEditor(field: FullField, values: MutableMap<String, Any?>) {
    when (field.kind) {
        FullFieldKind.Boolean -> SettingSwitch(field.label, values[field.key] as? Boolean ?: false) { values[field.key] = it }
        FullFieldKind.Choice -> Column { Text(field.label, style = MaterialTheme.typography.labelLarge); Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.spacedBy(6.dp)) { field.options.forEach { (key, label) -> FilterChip(selected = values[field.key]?.toString() == key, onClick = { values[field.key] = key }, label = { Text(label) }) } } }
        else -> {
            if (field.kind == FullFieldKind.Json) {
                Card(colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.errorContainer.copy(alpha = .55f))) {
                    Text("⚠ Расширенное поле. Ошибка в структуре может повредить данные карточки. Рекомендуется заполнять его на сайте.", modifier = Modifier.padding(12.dp), color = MaterialTheme.colorScheme.onErrorContainer, style = MaterialTheme.typography.bodySmall)
                }
                Spacer(Modifier.height(6.dp))
            }
            var text by remember(field.key) { mutableStateOf(valueToEditorText(values[field.key], field.kind)) }
            OutlinedTextField(
                value = text,
                onValueChange = { next -> text = next; values[field.key] = editorTextToValue(next, field.kind, values[field.key]) },
                label = { Text(field.label) }, modifier = Modifier.fillMaxWidth(),
                minLines = if (field.kind in setOf(FullFieldKind.Multiline, FullFieldKind.Json, FullFieldKind.Lines)) 3 else 1,
                keyboardOptions = KeyboardOptions(keyboardType = if (field.kind in setOf(FullFieldKind.Number, FullFieldKind.Integer)) KeyboardType.Decimal else KeyboardType.Text),
                supportingText = when (field.kind) { FullFieldKind.Date -> { { Text("Формат: ГГГГ-ММ-ДД") } }; FullFieldKind.Lines -> { { Text("По одному значению в строке") } }; FullFieldKind.Json -> { { Text("Сложная таблица в формате JSON; исходная структура сохраняется") } }; else -> null },
            )
        }
    }
}

private fun fieldVisible(key: String, values: Map<String, Any?>): Boolean {
    val costume = values["costume_type"]?.toString()
    val sewing = values["sewing_type"]?.toString()
    if (key in setOf("sewing_type", "sewing_fabric", "sewing_hardware", "sewing_pattern", "sewing_mockup", "sewing_fitting", "sewing_details", "costume_executor", "costume_deadline", "costume_prepayment", "costume_postpayment", "costume_fabric_price", "costume_hardware_price", "costume_fabric_rows_json", "costume_hardware_rows_json") && costume != "sew") return false
    if (key in setOf("sewing_fabric", "sewing_hardware", "sewing_pattern", "sewing_mockup", "sewing_fitting", "sewing_details", "costume_fabric_price", "costume_hardware_price", "costume_fabric_rows_json", "costume_hardware_rows_json") && sewing != "self") return false
    if (key in setOf("costume_executor", "costume_prepayment", "costume_postpayment") && sewing != "outsourced") return false
    if (key in setOf("costume_bought", "costume_link", "costume_buy_price") && costume != "buy") return false
    if (key in setOf("lenses_color", "lenses_comment", "lenses_price", "lenses_currency") && values["lenses_enabled"] != true) return false
    val wig = values["wig_type"]?.toString()
    if (key in setOf("wigmaker_name", "wig_price", "wig_deadline") && wig != "wigmaker") return false
    if (key in setOf("wig_link", "wig_buy_price") && wig != "buy") return false
    if (key in setOf("wig_no_buy_from", "wig_restyle") && wig != "no_buy") return false
    val craft = values["craft_type"]?.toString()
    if (key in setOf("craft_master", "craft_price", "craft_deadline") && craft != "order") return false
    if (key in setOf("craft_material_price", "craft_parts_json") && craft != "self") return false
    if (key in setOf("project_leader", "cosbands_json", "project_deadline", "related_cards_json", "project_characters_json", "coproplayers_json", "coproplayer_nicks_json") && values["plan_type"]?.toString() != "project") return false
    return true
}

private fun valueToEditorText(value: Any?, kind: FullFieldKind): String = when {
    value == null || value == JSONObject.NULL -> ""
    kind == FullFieldKind.Lines && value is List<*> -> value.joinToString("\n") { it?.toString().orEmpty() }
    kind == FullFieldKind.Json -> when (value) { is List<*> -> JSONArray(value).toString(2); is Map<*, *> -> JSONObject(value).toString(2); else -> value.toString() }
    else -> value.toString()
}

private fun editorTextToValue(text: String, kind: FullFieldKind, oldValue: Any?): Any? = when (kind) {
    FullFieldKind.Number -> text.replace(',', '.').toDoubleOrNull()
    FullFieldKind.Integer -> text.toIntOrNull()
    FullFieldKind.Lines -> text.lines().map { it.trim() }.filter { it.isNotEmpty() }
    FullFieldKind.Json -> runCatching { if (text.trim().startsWith("[")) jsonArrayToList(JSONArray(text)) else jsonObjectToMap(JSONObject(text)) }.getOrElse { oldValue }
    else -> text.trim().ifBlank { null }
}

private fun jsonObjectToMap(json: JSONObject): Map<String, Any?> = json.keys().asSequence().associateWith { key -> jsonValue(json.opt(key)) }
private fun jsonArrayToList(json: JSONArray): List<Any?> = (0 until json.length()).map { jsonValue(json.opt(it)) }
private fun jsonValue(value: Any?): Any? = when (value) { is JSONObject -> jsonObjectToMap(value); is JSONArray -> jsonArrayToList(value); JSONObject.NULL -> null; else -> value }
