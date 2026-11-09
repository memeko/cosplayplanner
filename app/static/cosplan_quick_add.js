(function () {
  const dialog = document.getElementById("quick-cosplan-dialog");
  const openButton = document.getElementById("quick-cosplan-open");
  const closeButton = document.getElementById("quick-cosplan-close");
  const form = document.getElementById("quick-cosplan-form");
  const status = document.getElementById("quick-cosplan-status");
  if (!dialog || !openButton || !closeButton || !form || !status) return;

  openButton.addEventListener("click", function () {
    status.textContent = "";
    status.removeAttribute("data-kind");
    dialog.showModal();
  });
  closeButton.addEventListener("click", function () {
    dialog.close();
  });
  dialog.addEventListener("click", function (event) {
    if (event.target === dialog) dialog.close();
  });

  function increment(selector) {
    document.querySelectorAll(selector).forEach(function (node) {
      const value = Number.parseInt(node.textContent || "0", 10);
      node.textContent = String((Number.isFinite(value) ? value : 0) + 1);
    });
  }

  form.addEventListener("submit", async function (event) {
    event.preventDefault();
    const submitButton = form.querySelector('button[type="submit"]');
    status.textContent = "Сохраняю идею…";
    status.removeAttribute("data-kind");
    if (submitButton) submitButton.disabled = true;

    try {
      const response = await fetch(form.action, {
        method: "POST",
        body: new FormData(form),
        headers: { Accept: "application/json" },
      });
      const payload = await response.json();
      if (!response.ok || !payload.ok) {
        throw new Error(payload.error || "Не удалось создать карточку.");
      }

      const view = String(payload.view || "cards");
      let container = document.querySelector(view === "list" ? ".cosplan-list-view" : ".cards-grid");
      if (!container) {
        const emptyState = document.querySelector(".cosplan-empty-state");
        container = document.createElement("div");
        container.className = view === "list" ? "cosplan-list-view" : "cards-grid";
        if (emptyState) emptyState.replaceWith(container);
        else form.parentElement.insertBefore(container, dialog);
      }
      container.insertAdjacentHTML("afterbegin", payload.html);
      increment("[data-cosplan-visible-count]");
      increment("[data-cosplan-total-count]");
      increment("[data-cosplan-current-count]");
      form.reset();
      status.textContent = "Идея добавлена.";
      window.setTimeout(function () { dialog.close(); }, 450);
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : "Не удалось создать карточку.";
      status.dataset.kind = "error";
    } finally {
      if (submitButton) submitButton.disabled = false;
    }
  });
})();
