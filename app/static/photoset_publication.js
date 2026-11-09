(function () {
  const form = document.getElementById("photoset-publication-form");
  const rows = form && form.querySelector("[data-publication-rows]");
  const template = document.getElementById("photoset-publication-row-template");
  if (!form || !rows || !template) return;

  let sequence = Date.now();

  function prepareRow(row) {
    const idInput = row.querySelector('input[name="publication_row_id"]');
    const rowId = (row.dataset.rowId || (idInput && idInput.value) || `publication-${sequence++}`).replace(/[^a-zA-Z0-9_-]/g, "");
    row.dataset.rowId = rowId;
    if (idInput) idInput.value = rowId;
    row.querySelectorAll('input[type="checkbox"]').forEach(function (checkbox) {
      checkbox.value = rowId;
    });
  }

  rows.querySelectorAll(".photoset-publication-row").forEach(prepareRow);

  form.addEventListener("click", function (event) {
    const addButton = event.target.closest("[data-add-publication-row]");
    if (addButton) {
      const fragment = template.content.cloneNode(true);
      const row = fragment.querySelector(".photoset-publication-row");
      prepareRow(row);
      rows.append(fragment);
      row.querySelector('input[name="publication_frame"]').focus();
      return;
    }

    const removeButton = event.target.closest("[data-remove-publication-row]");
    if (removeButton) {
      removeButton.closest(".photoset-publication-row").remove();
    }
  });
})();
