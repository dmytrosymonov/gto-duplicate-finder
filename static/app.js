(function () {
  const apikeyInput = document.getElementById('apikey');
  const rpsInput = document.getElementById('rps');
  const saveApikeyBtn = document.getElementById('saveApikey');
  const countrySelect = document.getElementById('country');
  const citySearchInput = document.getElementById('citySearch');
  const cityListEl = document.getElementById('cityList');
  const selectedCitiesEl = document.getElementById('selectedCities');
  const findDuplicatesBtn = document.getElementById('findDuplicates');
  const findErrorsBtn = document.getElementById('findErrors');
  const progressSection = document.getElementById('progressSection');
  const resultsSection = document.getElementById('resultsSection');
  const hotelsLoaded = document.getElementById('hotelsLoaded');
  const comparisonsDone = document.getElementById('comparisonsDone');
  const flagsFound = document.getElementById('flagsFound');
  const rpsDisplay = document.getElementById('rpsDisplay');
  const apiRequests = document.getElementById('apiRequests');
  const avgResponse = document.getElementById('avgResponse');
  const peakResponse = document.getElementById('peakResponse');
  const progressFill = document.getElementById('progressFill');
  const progressPctEl = document.getElementById('progressPct');
  const resultsBody = document.getElementById('resultsBody');
  const filterType = document.getElementById('filterType');
  const sortBy = document.getElementById('sortBy');
  const exportExcelBtn = document.getElementById('exportExcel');
  const loadHistoryBtn = document.getElementById('loadHistory');
  const historyListEl = document.getElementById('historyList');
  const stopScanBtn = document.getElementById('stopScan');
  const elapsedTimeEl = document.getElementById('elapsedTime');
  const remainingTimeEl = document.getElementById('remainingTime');

  let allResults = [];
  let resultType = 'duplicates';

  if (loadHistoryBtn && historyListEl) {
    loadHistoryBtn.addEventListener('click', loadHistory);
  }

  function loadHistory() {
    fetch('/api/scan/history', { credentials: 'include' })
      .then(r => r.json())
      .then(data => {
        const items = data.data || [];
        if (!items.length) {
          historyListEl.innerHTML = '<p class="hint">Нет завершённых сканов.</p>';
          return;
        }
        historyListEl.innerHTML = '<table class="history-table"><thead><tr><th>Города</th><th>Флагов</th><th>Дата</th><th></th></tr></thead><tbody>' +
          items.map(h => {
            const dt = h.done_at ? new Date(h.done_at * 1000).toLocaleString() : '';
            return '<tr><td>' + escapeHtml(h.cities_label || (h.city_ids ? h.city_ids.join(', ') : '')) + '</td><td>' + (h.flags_count || 0) + '</td><td>' + escapeHtml(dt) + '</td><td><button type="button" class="btn-view" data-scan-id="' + escapeHtml(h.scan_id) + '">Показать</button></td></tr>';
          }).join('') + '</tbody></table>';
        historyListEl.querySelectorAll('.btn-view').forEach(btn => {
          btn.addEventListener('click', () => {
            const sid = btn.getAttribute('data-scan-id');
            fetch('/api/scan/result?scan_id=' + encodeURIComponent(sid), { credentials: 'include' })
              .then(r => r.json())
              .then(d => {
                allResults = d.results || [];
                resultType = d.result_type || 'duplicates';
                updateTableStructure();
                applyFilterAndSort();
                if (exportExcelBtn) exportExcelBtn.disabled = false;
                document.getElementById('resultsSection').scrollIntoView();
              })
              .catch(() => alert('Ошибка загрузки результата'));
          });
        });
      })
      .catch(() => alert('Ошибка загрузки истории. Убедитесь, что API key задан.'));
  }
  let pollInterval = null;
  let allCities = [];
  let selectedCities = [];

  if (countrySelect && !countrySelect.disabled) {
    loadCountries();
  }

  function getRps() {
    const v = parseFloat(rpsInput.value);
    return isNaN(v) || v <= 0 ? 5 : Math.min(50, v);
  }

  saveApikeyBtn.addEventListener('click', async () => {
    const key = (apikeyInput.value || '').trim();
    if (!key) {
      alert('Введите API key');
      return;
    }
    try {
      const r = await fetch('/api/apikey', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ apikey: key })
      });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        throw new Error(d.detail || r.statusText);
      }
      countrySelect.disabled = false;
      countrySelect.innerHTML = '<option value="">-- Загрузка стран... --</option>';
      loadCountries();
    } catch (e) {
      alert('Ошибка: ' + e.message);
    }
  });

  async function loadCountries() {
    try {
      const r = await fetch('/api/countries?rps=' + getRps(), { credentials: 'include' });
      if (!r.ok) {
        const d = await r.json().catch(() => ({}));
        if (r.status === 401) throw new Error('Сначала сохраните API key');
        throw new Error(d.detail || 'Ошибка загрузки стран');
      }
      const data = await r.json();
      let items = (data.data || []).slice();
      items.sort((a, b) => (a.name || '').localeCompare(b.name || '', undefined, { sensitivity: 'base' }));
      countrySelect.innerHTML = '<option value="">-- Выберите страну --</option>' +
        items.map(c => `<option value="${c.id}">${escapeHtml(c.name)}</option>`).join('');
    } catch (e) {
      countrySelect.innerHTML = '<option value="">Ошибка: ' + escapeHtml(e.message) + '</option>';
    }
  }

  countrySelect.addEventListener('change', () => {
    const id = countrySelect.value;
    citySearchInput.disabled = !id;
    selectedCities = [];
    allCities = [];
    renderSelectedCities();
    renderCityList();
    updateScanButtonsState();
    if (!id) {
      cityListEl.innerHTML = '';
      return;
    }
    cityListEl.innerHTML = '<div class="city-list-item">Загрузка городов...</div>';
    fetch('/api/cities?country_id=' + id + '&rps=' + getRps(), { credentials: 'include' })
      .then(r => {
        if (r.status === 401) throw new Error('Сначала сохраните API key');
        if (!r.ok) throw new Error('Ошибка загрузки городов');
        return r.json();
      })
      .then(data => {
        allCities = (data.data || []).map(c => ({
          id: Number(c.id) || c.id,
          name: c.name || ''
        }));
        allCities.sort((a, b) => (a.name || '').localeCompare(b.name || '', undefined, { sensitivity: 'base' }));
        renderCityList();
      })
      .catch(e => {
        cityListEl.innerHTML = '<div class="city-list-item">Ошибка: ' + escapeHtml(e.message) + '</div>';
      });
  });

  citySearchInput.addEventListener('input', () => {
    renderCityList();
  });

  function getSearchQuery() {
    return (citySearchInput.value || '').trim().toLowerCase();
  }

  function renderCityList() {
    const q = getSearchQuery();
    const filtered = q
      ? allCities.filter(c => (c.name || '').toLowerCase().includes(q))
      : allCities;
    const selectedIds = new Set(selectedCities.map(c => String(c.id)));
    cityListEl.innerHTML = filtered.map(c => {
      const sid = String(c.id);
      const sel = selectedIds.has(sid) ? ' selected' : '';
      return '<div class="city-list-item' + sel + '" data-id="' + sid + '">' + escapeHtml(c.name) + '</div>';
    }).join('');
  }

  cityListEl.addEventListener('click', function(e) {
    const item = e.target.closest('.city-list-item');
    if (!item) return;
    const idStr = item.getAttribute('data-id');
    if (!idStr) return;
    const id = parseInt(idStr, 10);
    const city = allCities.find(c => String(c.id) === idStr || c.id === id);
    if (!city) return;
    const idx = selectedCities.findIndex(c => String(c.id) === idStr || c.id === id);
    if (idx >= 0) {
      selectedCities.splice(idx, 1);
    } else {
      selectedCities.push({ id: id, name: city.name || String(city.id) });
    }
    renderSelectedCities();
    renderCityList();
    updateScanButtonsState();
  });

  function renderSelectedCities() {
    selectedCitiesEl.innerHTML = selectedCities.map(c =>
      '<span class="city-chip">' + escapeHtml(c.name) +
      ' <button type="button" data-id="' + c.id + '">&times;</button></span>'
    ).join('');
  }

  selectedCitiesEl.addEventListener('click', function(e) {
    const btn = e.target.closest('.city-chip button');
    if (!btn) return;
    e.stopPropagation();
    const idStr = btn.getAttribute('data-id');
    if (!idStr) return;
    selectedCities = selectedCities.filter(c => String(c.id) !== idStr);
    renderSelectedCities();
    renderCityList();
    updateScanButtonsState();
  });

  function updateScanButtonsState() {
    const disabled = selectedCities.length === 0;
    if (findDuplicatesBtn) findDuplicatesBtn.disabled = disabled;
    if (findErrorsBtn) findErrorsBtn.disabled = disabled;
  }

  async function startScan(scanType) {
    const cityIds = selectedCities.map(c => c.id).filter(id => id != null && !isNaN(id));
    const countryId = countrySelect.value || null;
    if (!cityIds.length) {
      alert('Выберите хотя бы один город');
      return;
    }
    const rps = getRps();
    rpsDisplay.textContent = rps;
    progressSection.hidden = false;
    hotelsLoaded.textContent = '0';
    comparisonsDone.textContent = '0';
    flagsFound.textContent = '0';
    progressFill.style.width = '0%';
    if (progressPctEl) progressPctEl.textContent = '0%';
    allResults = [];
    resultType = scanType;
    updateTableStructure();
    renderTable([]);
    if (exportExcelBtn) exportExcelBtn.disabled = true;
    if (findDuplicatesBtn) findDuplicatesBtn.disabled = true;
    if (findErrorsBtn) findErrorsBtn.disabled = true;
    if (stopScanBtn) {
      stopScanBtn.hidden = false;
      stopScanBtn.disabled = false;
      stopScanBtn.textContent = 'Остановить';
    }
    if (elapsedTimeEl) elapsedTimeEl.textContent = '';
    if (remainingTimeEl) remainingTimeEl.textContent = '';

    const payload = {
      city_ids: cityIds,
      country_id: countryId ? parseInt(countryId) : null,
      rps: rps,
      scan_type: scanType
    };
    if (cityIds.length === 1) payload.city_id = cityIds[0];

    try {
      const r = await fetch('/api/scan', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const respBody = await r.json().catch(() => ({}));
      if (!r.ok) {
        const msg = Array.isArray(respBody.detail)
          ? respBody.detail.map(d => d.msg || JSON.stringify(d)).join('; ')
          : (respBody.detail || respBody.message || r.statusText);
        throw new Error(msg);
      }
      const currentScanId = respBody.scan_id || null;
      pollInterval = setInterval(() => pollStatus(currentScanId), 1500);
      pollStatus(currentScanId);
    } catch (e) {
      alert('Ошибка: ' + e.message);
      updateScanButtonsState();
    }
  }

  if (findDuplicatesBtn) {
    findDuplicatesBtn.addEventListener('click', () => startScan('duplicates'));
  }
  if (findErrorsBtn) {
    findErrorsBtn.addEventListener('click', () => startScan('errors'));
  }

  if (stopScanBtn) {
    stopScanBtn.addEventListener('click', async () => {
      stopScanBtn.disabled = true;
      try {
        const r = await fetch('/api/scan/cancel', {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({})
        });
        const d = await r.json().catch(() => ({}));
        if (d.status === 'cancelling') {
          stopScanBtn.textContent = 'Останавливаем...';
        } else {
          stopScanBtn.disabled = false;
        }
      } catch (e) {
        stopScanBtn.disabled = false;
      }
    });
  }

  function formatDuration(seconds) {
    if (seconds < 60) return Math.round(seconds) + ' сек';
    const m = Math.floor(seconds / 60);
    const s = Math.round(seconds % 60);
    if (m < 60) return s > 0 ? m + ' мин ' + s + ' сек' : m + ' мин';
    const h = Math.floor(m / 60);
    const mm = m % 60;
    return (mm > 0 ? h + ' ч ' + mm + ' мин' : h + ' ч');
  }

  function pollStatus(scanId) {
    const url = scanId ? '/api/scan/status?scan_id=' + encodeURIComponent(scanId) : '/api/scan/status';
    fetch(url, { credentials: 'include' })
      .then(r => r.json())
      .then(data => {
        hotelsLoaded.textContent = data.hotels_loaded || 0;
        comparisonsDone.textContent = data.comparisons_done || 0;
        flagsFound.textContent = data.flags_found || 0;
        if (data.status === 'queued') {
          progressPctEl.textContent = 'В очереди...';
          if (elapsedTimeEl) elapsedTimeEl.textContent = '';
          if (remainingTimeEl) remainingTimeEl.textContent = '';
        } else {
          const pct = data.progress_pct != null ? data.progress_pct : 0;
          progressFill.style.width = pct + '%';
          progressPctEl.textContent = pct + '%';
          const startedAt = data.started_at;
          if (startedAt && elapsedTimeEl) {
            const elapsed = (Date.now() / 1000) - startedAt;
            elapsedTimeEl.textContent = 'Прошло: ' + formatDuration(elapsed);
          }
          if (startedAt && remainingTimeEl && pct > 0 && pct < 100) {
            const elapsed = (Date.now() / 1000) - startedAt;
            const totalEst = elapsed / (pct / 100);
            const remaining = totalEst - elapsed;
            remainingTimeEl.textContent = 'Осталось ~' + formatDuration(remaining);
          } else if (remainingTimeEl && (pct >= 100 || data.done)) {
            remainingTimeEl.textContent = '';
          } else if (remainingTimeEl && !startedAt) {
            remainingTimeEl.textContent = '';
          }
        }
        if (data.stats) {
          apiRequests.textContent = data.stats.request_count || 0;
          avgResponse.textContent = (data.stats.avg_response_ms || 0).toFixed(1);
          peakResponse.textContent = (data.stats.peak_response_ms || 0).toFixed(1);
        }
        if (data.done) {
          clearInterval(pollInterval);
          pollInterval = null;
          if (stopScanBtn) {
            stopScanBtn.hidden = true;
            stopScanBtn.disabled = true;
          }
          if (remainingTimeEl) remainingTimeEl.textContent = '';
          if (elapsedTimeEl && data.started_at) {
            const elapsed = (Date.now() / 1000) - data.started_at;
            elapsedTimeEl.textContent = 'Выполнено за: ' + formatDuration(elapsed);
          } else if (elapsedTimeEl) elapsedTimeEl.textContent = '';
          updateScanButtonsState();
          progressFill.style.width = '100%';
          if (progressPctEl) progressPctEl.textContent = '100%';
          if (data.results) {
            allResults = data.results;
            resultType = data.result_type || 'duplicates';
            updateTableStructure();
            applyFilterAndSort();
            if (exportExcelBtn) exportExcelBtn.disabled = false;
          }
          if (data.error) {
            alert('Ошибка сканирования: ' + data.error);
          }
        }
      })
      .catch(() => {});
  }

  function escapeHtml(s) {
    if (s == null) return '';
    const div = document.createElement('div');
    div.textContent = s;
    return div.innerHTML;
  }

  function applyFilterAndSort() {
    renderTable(getFilteredResults());
  }

  const HOTEL_LINK = 'https://dev.gto.ua/backend.php/hotel/edit?id=';

  function linkify(id) {
    if (id == null) return '';
    return '<a href="' + HOTEL_LINK + id + '" target="_blank">' + escapeHtml(String(id)) + '</a>';
  }

  function updateTableStructure() {
    const thead = document.querySelector('#resultsTable thead tr');
    const filtersEl = document.getElementById('filters');
    if (!thead || !filtersEl) return;
    if (resultType === 'errors') {
      thead.innerHTML = '<th>Название отеля</th><th>ID</th><th>Звёздность</th>';
      filtersEl.style.display = 'none';
    } else {
      thead.innerHTML = '<th data-sort="hotel_name">Название отеля</th><th data-sort="id1">ID 1</th><th data-sort="id2">ID 2</th><th data-sort="address">Адрес</th><th data-sort="score">Общий скоринг</th><th data-sort="reason">Причина флага</th>';
      filtersEl.style.display = '';
      document.querySelectorAll('#resultsTable th[data-sort]').forEach(th => {
        th.addEventListener('click', () => {
          const col = th.getAttribute('data-sort');
          if (col === 'reason') sortBy.value = 'reason';
          else if (col === 'score' || col === 'id1' || col === 'id2') sortBy.value = 'score';
          applyFilterAndSort();
        });
      });
    }
  }

  function renderTable(rows) {
    if (resultType === 'errors') {
      resultsBody.innerHTML = rows.map(r => `
        <tr>
          <td>${escapeHtml(r.hotel_name || '')}</td>
          <td>${linkify(r.id1)}</td>
          <td>${escapeHtml(r.stars || '')}</td>
        </tr>
      `).join('');
    } else {
      resultsBody.innerHTML = rows.map(r => {
        const id2Val = r.id2;
        const id2Html = Array.isArray(id2Val)
          ? id2Val.map(id => linkify(id)).join(', ')
          : linkify(id2Val);
        const score = r.confidence_score != null ? r.confidence_score.toFixed(3) : '';
        return `<tr data-flag-type="${escapeHtml(r.flag_type || '')}">
          <td>${escapeHtml(r.hotel_name || '')}</td>
          <td>${linkify(r.id1)}</td>
          <td>${id2Html}</td>
          <td>${escapeHtml(r.address || '')}</td>
          <td>${score}</td>
          <td>${escapeHtml(r.reason || '')}</td>
        </tr>`;
      }).join('');
    }
  }

  function getFilteredResults() {
    let rows = allResults.slice();
    if (resultType === 'errors') return rows;
    const ft = filterType.value;
    if (ft !== 'all') rows = rows.filter(r => r.flag_type === ft);
    const sb = sortBy.value;
    if (sb === 'score') rows.sort((a, b) => (b.confidence_score || 0) - (a.confidence_score || 0));
    else rows.sort((a, b) => (a.reason || '').localeCompare(b.reason || ''));
    return rows;
  }

  exportExcelBtn.addEventListener('click', async () => {
    const rows = getFilteredResults();
    if (!rows.length) {
      alert('Нет данных для экспорта');
      return;
    }
    try {
      const r = await fetch('/api/export/excel', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ results: rows, result_type: resultType })
      });
      if (!r.ok) throw new Error('Ошибка экспорта');
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = resultType === 'errors' ? 'error_descriptions.xlsx' : 'duplicates.xlsx';
      a.click();
      URL.revokeObjectURL(url);
    } catch (e) {
      alert('Ошибка экспорта: ' + e.message);
    }
  });

  filterType.addEventListener('change', applyFilterAndSort);
  sortBy.addEventListener('change', applyFilterAndSort);

  document.querySelectorAll('#resultsTable th[data-sort]').forEach(th => {
    th.addEventListener('click', () => {
      const col = th.getAttribute('data-sort');
      if (col === 'reason') sortBy.value = 'reason';
      else if (col === 'score' || col === 'id1' || col === 'id2') sortBy.value = 'score';
      applyFilterAndSort();
    });
  });
})();
