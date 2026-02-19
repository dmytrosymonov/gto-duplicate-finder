(function () {
  const apikeyInput = document.getElementById('apikey');
  const rpsInput = document.getElementById('rps');
  const saveApikeyBtn = document.getElementById('saveApikey');
  const countrySelect = document.getElementById('country');
  const citySearchInput = document.getElementById('citySearch');
  const cityListEl = document.getElementById('cityList');
  const selectedCitiesEl = document.getElementById('selectedCities');
  const startScanBtn = document.getElementById('startScan');
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

  let allResults = [];
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
      const r = await fetch('/api/countries?rps=' + getRps());
      if (!r.ok) throw new Error('Ошибка загрузки стран');
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
    updateStartScanState();
    if (!id) {
      cityListEl.innerHTML = '';
      return;
    }
    cityListEl.innerHTML = '<div class="city-list-item">Загрузка городов...</div>';
    fetch('/api/cities?country_id=' + id + '&rps=' + getRps())
      .then(r => r.json())
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
    updateStartScanState();
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
    updateStartScanState();
  });

  function updateStartScanState() {
    startScanBtn.disabled = selectedCities.length === 0;
  }

  startScanBtn.addEventListener('click', async () => {
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
    renderTable([]);
    if (exportExcelBtn) exportExcelBtn.disabled = true;
    startScanBtn.disabled = true;

    try {
      const payload = {
        city_ids: cityIds,
        country_id: countryId ? parseInt(countryId) : null,
        rps: rps
      };
      if (cityIds.length === 1) payload.city_id = cityIds[0];
      const r = await fetch('/api/scan', {
        method: 'POST',
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
      pollInterval = setInterval(pollStatus, 1500);
      pollStatus();
    } catch (e) {
      alert('Ошибка: ' + e.message);
      startScanBtn.disabled = false;
    }
  });

  function pollStatus() {
    fetch('/api/scan/status')
      .then(r => r.json())
      .then(data => {
        hotelsLoaded.textContent = data.hotels_loaded || 0;
        comparisonsDone.textContent = data.comparisons_done || 0;
        flagsFound.textContent = data.flags_found || 0;
        const pct = data.progress_pct != null ? data.progress_pct : 0;
        progressFill.style.width = pct + '%';
        if (progressPctEl) progressPctEl.textContent = pct + '%';
        if (data.stats) {
          apiRequests.textContent = data.stats.request_count || 0;
          avgResponse.textContent = (data.stats.avg_response_ms || 0).toFixed(1);
          peakResponse.textContent = (data.stats.peak_response_ms || 0).toFixed(1);
        }
        if (data.done) {
          clearInterval(pollInterval);
          pollInterval = null;
          startScanBtn.disabled = false;
          progressFill.style.width = '100%';
          if (progressPctEl) progressPctEl.textContent = '100%';
          if (data.results) {
            allResults = data.results;
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

  function renderTable(rows) {
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

  function getFilteredResults() {
    let rows = allResults.slice();
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
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ results: rows })
      });
      if (!r.ok) throw new Error('Ошибка экспорта');
      const blob = await r.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'duplicates.xlsx';
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
