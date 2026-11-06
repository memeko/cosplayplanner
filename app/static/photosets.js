(function () {
  const form = document.getElementById('photoset-form'); if (!form) return;
  const money = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 2 });
  function renumberStoryboard() { document.querySelectorAll('.storyboard-row').forEach((row, i) => { const id='story-'+i; row.querySelector('[name="photoset_storyboard_row_id"]').value=id; const done=row.querySelector('[name="photoset_storyboard_done"]'); done.value=id; }); const out=document.querySelector('[data-story-count]'); if(out) out.textContent=document.querySelectorAll('.storyboard-row').length; }
  function calculate() { const n = name => Number(form.elements[name]?.value || 0); let total=n('hourly_price')*n('rental_hours')+n('dressing_room_hours')*n('dressing_room_hourly_price'); if(form.elements.smoke_machine_required?.checked) total+=n('smoke_machine_price'); form.querySelectorAll('[name="extra_price"]').forEach(input=>total+=Number(input.value||0)); document.querySelector('[data-total]').textContent=money.format(total)+' ₽'; const payers=Number(form.elements.payers_count?.value||0); document.querySelector('[data-contribution]').textContent=money.format(payers>0?total/payers:0)+' ₽'; }
  form.addEventListener('click', e => { const add=e.target.closest('[data-add]'); if(add){ const key=add.dataset.add, tpl=document.getElementById('tpl-'+key); form.querySelector('[data-rows="'+key+'"]').append(tpl.content.cloneNode(true)); renumberStoryboard(); calculate(); } const remove=e.target.closest('[data-remove]'); if(remove){ remove.closest('.repeat-row').remove(); renumberStoryboard(); calculate(); } });
  form.addEventListener('input', calculate); form.addEventListener('change', calculate); renumberStoryboard(); calculate();
})();

(function () {
  const form = document.getElementById('photoset-form'); if (!form) return;
  const steps = Array.from(form.querySelectorAll('[data-form-step]'));
  const pageButtons = Array.from(form.querySelectorAll('[data-step-go]'));
  let currentStep = 0;
  function showStep(index) {
    currentStep = Math.max(0, Math.min(steps.length - 1, index));
    steps.forEach((step, i) => { step.hidden = i !== currentStep; });
    pageButtons.forEach((button, i) => button.classList.toggle('active', i === currentStep));
    form.querySelector('[data-step-prev]').hidden = currentStep === 0;
    form.querySelector('[data-step-next]').hidden = currentStep === steps.length - 1;
    form.querySelector('[data-step-submit]').hidden = currentStep !== steps.length - 1;
    form.querySelector('[data-step-label]').textContent = `Страница ${currentStep + 1} из ${steps.length}`;
    form.querySelector('.photoset-form-pagination')?.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  }
  pageButtons.forEach(button => button.addEventListener('click', () => showStep(Number(button.dataset.stepGo))));
  form.querySelector('[data-step-prev]').addEventListener('click', () => showStep(currentStep - 1));
  form.querySelector('[data-step-next]').addEventListener('click', () => showStep(currentStep + 1));
  form.addEventListener('invalid', event => {
    const step = event.target.closest('[data-form-step]');
    if (step) showStep(steps.indexOf(step));
  }, true);
  showStep(0);

  const uniqueRoles = raw => {
    const seen = new Set(); const roles = [];
    String(raw || '').split(/[\n,;]+/).map(value => value.trim()).filter(Boolean).forEach(value => {
      const key = value.toLowerCase(); if (!seen.has(key)) { seen.add(key); roles.push(value); }
    }); return roles;
  };
  const combinations = (items, size) => {
    const result = []; const walk = (start, row) => { if (row.length === size) { result.push(row.slice()); return; } for (let i = start; i < items.length; i += 1) { row.push(items[i]); walk(i + 1, row); row.pop(); } };
    if (items.length >= size) walk(0, []); return result;
  };
  function distribute(totalValue, roles) {
    const total = Math.max(0, Math.floor(totalValue)); if (!total) return { error: 'Укажите количество кадров больше нуля.' }; if (!roles.length) return { error: 'Добавьте хотя бы одну роль.' };
    const solo = new Array(roles.length).fill(0), pairs = combinations(roles, 2), triples = combinations(roles, 3), pairCounts = new Array(pairs.length).fill(0), tripleCounts = new Array(triples.length).fill(0), warnings = [];
    let remaining = total, all = 0;
    if (roles.length > 3 && remaining > 0) { all = 1; remaining -= 1; }
    if (roles.length === 2) { let i = 0; while (remaining > 0) { if (i % 3 < 2) solo[i % 3] += 1; else pairCounts[0] += 1; i += 1; remaining -= 1; } }
    else { for (let i = 0; i < roles.length && remaining > 0; i += 1) { solo[i] += 1; remaining -= 1; } }
    if (roles.length > 3 && total < roles.length + 1) warnings.push('Кадров мало: невозможно дать всем ролям сольный кадр и сохранить отдельный общий кадр.'); else if (total < roles.length) warnings.push('Кадров мало: не всем ролям хватит сольных кадров.');
    let pairIndex = 0, tripleIndex = 0;
    while (roles.length !== 2 && remaining > 0) { if (pairs.length) { pairCounts[pairIndex++ % pairs.length] += 1; remaining -= 1; if (!remaining) break; } if (triples.length) { tripleCounts[tripleIndex++ % triples.length] += 1; remaining -= 1; if (!remaining) break; } if (!pairs.length && !triples.length) { solo[(total - remaining) % roles.length] += 1; remaining -= 1; } }
    return { total, roles, solo, pairs, triples, pairCounts, tripleCounts, all, warnings };
  }
  let lastDistribution = null;
  const result = form.querySelector('[data-calc-result]');
  function render(distribution) {
    if (distribution.error) { result.textContent = distribution.error; return; }
    const rows = distribution.roles.map((role, i) => `${role}: ${distribution.solo[i]} сольных`);
    distribution.pairs.forEach((combo, i) => { if (distribution.pairCounts[i]) rows.push(`${combo.join(' + ')}: ${distribution.pairCounts[i]} парных`); });
    distribution.triples.forEach((combo, i) => { if (distribution.tripleCounts[i]) rows.push(`${combo.join(' + ')}: ${distribution.tripleCounts[i]} тройных`); });
    if (distribution.all) rows.unshift(`Общий кадр со всеми ролями: ${distribution.all}`);
    result.innerHTML = `<strong>Всего кадров: ${distribution.total}</strong><ul>${rows.map(row => `<li>${row}</li>`).join('')}</ul>${distribution.warnings.map(row => `<p>${row}</p>`).join('')}`;
  }
  form.querySelector('[data-fill-roles]').addEventListener('click', () => { form.querySelector('[data-calc-roles]').value = Array.from(form.querySelectorAll('[name="character"]')).map(input => input.value.trim()).filter(Boolean).join('\n'); });
  form.querySelector('[data-calc-run]').addEventListener('click', () => { const value = distribute(Number(form.querySelector('[data-calc-total]').value || 0), uniqueRoles(form.querySelector('[data-calc-roles]').value)); lastDistribution = value.error ? null : value; render(value); });
  form.querySelector('[data-calc-build]').addEventListener('click', () => {
    if (!lastDistribution) { result.textContent = 'Сначала выполните расчёт.'; return; }
    const target = form.querySelector('[data-rows="storyboard"]'); target.innerHTML = '';
    const append = (who, count) => { for (let i = 0; i < count; i += 1) { const fragment = document.getElementById('tpl-storyboard').content.cloneNode(true); fragment.querySelector('[name="photoset_storyboard_who"]').value = who; target.append(fragment); } };
    lastDistribution.roles.forEach((role, i) => append(role, lastDistribution.solo[i]));
    if (lastDistribution.all) append(`Общий кадр: ${lastDistribution.roles.join(' + ')}`, lastDistribution.all);
    lastDistribution.pairs.forEach((combo, i) => append(combo.join(' + '), lastDistribution.pairCounts[i]));
    lastDistribution.triples.forEach((combo, i) => append(combo.join(' + '), lastDistribution.tripleCounts[i]));
    target.dispatchEvent(new Event('input', { bubbles: true }));
    const count = target.querySelectorAll('.storyboard-row').length; form.querySelector('[data-story-count]').textContent = count;
    target.querySelectorAll('.storyboard-row').forEach((row, i) => { const id = `story-${i}`; row.querySelector('[name="photoset_storyboard_row_id"]').value = id; row.querySelector('[name="photoset_storyboard_done"]').value = id; });
  });
})();
