// Фестивали моего города — Scriptable
// Для городского виджета город можно задать в параметре виджета.
// Если параметр пустой, используется город по умолчанию.
const DAYS = 30;
const CITY = args.widgetParameter || "Москва";
const FEED_URL = "https://cosplay-planner.ru/api/scriptable/festivals";

function dateLabel(startValue, endValue) {
  const options = { day: "numeric", month: "short" };
  const start = new Date(startValue + "T12:00:00");
  const startText = new Intl.DateTimeFormat("ru-RU", options).format(start);
  if (!endValue || endValue === startValue) return startText;
  const end = new Date(endValue + "T12:00:00");
  const endText = new Intl.DateTimeFormat("ru-RU", options).format(end);
  return startText + "–" + endText;
}

async function loadFestivals() {
  const cityParam = CITY ? "&city=" + encodeURIComponent(CITY) : "";
  const request = new Request(FEED_URL + "?days=" + DAYS + cityParam);
  request.timeoutInterval = 20;
  request.headers = { "Accept": "application/json" };
  return await request.loadJSON();
}

async function createWidget() {
  const widget = new ListWidget();
  widget.backgroundColor = new Color("#241A35");
  widget.setPadding(12, 14, 12, 14);

  let data;
  try {
    data = await loadFestivals();
  } catch (error) {
    const header = widget.addText(CITY || "Все города");
    header.font = Font.boldSystemFont(14);
    header.textColor = Color.white();
    widget.addSpacer(8);
    const message = widget.addText("Лента временно недоступна");
    message.font = Font.systemFont(11);
    message.textColor = new Color("#D7CDE8");
    return widget;
  }

  const heading = CITY || data.city || "Все города";
  const header = widget.addText(heading);
  header.font = Font.boldSystemFont(14);
  header.textColor = Color.white();
  widget.addSpacer(7);

  const events = Array.isArray(data.events) ? data.events : [];
  const family = config.widgetFamily || "medium";
  const limit = family === "small" ? 2 : family === "large" ? 8 : 5;
  if (!events.length) {
    const empty = widget.addText("Фестивалей нет");
    empty.font = Font.systemFont(11);
    empty.textColor = new Color("#D7CDE8");
    return widget;
  }

  for (const event of events.slice(0, limit)) {
    const row = widget.addText(dateLabel(event.date, event.end_date) + " — " + event.name);
    row.font = Font.systemFont(11);
    row.textColor = new Color("#F4F0FA");
    row.lineLimit = 1;
    widget.addSpacer(4);
  }

  if (events.length > limit) {
    const more = widget.addText("ещё " + (events.length - limit));
    more.font = Font.systemFont(9);
    more.textColor = new Color("#B7A6D3");
  }
  return widget;
}

const widget = await createWidget();
if (config.runsInWidget) {
  Script.setWidget(widget);
} else {
  await widget.presentMedium();
}
Script.complete();
