/* Scaler Web UI - Client-side application */
"use strict";

// ── State ──
var ws = null;
var reconnectDelay = 500;
var workerRows = {};       // worker_id -> <tr> element
var workerSortField = null;  // current sort column field name
var workerSortAsc = true;    // sort direction
var lastWorkersData = [];    // latest workers array for re-sorting
var taskLogCount = 0;
var TASK_LOG_MAX_SIZE = 100;
var taskRowMap = {};  // task_id -> tr element for in-place updates
var streamBars = [];       // current bar data from server
var streamRows = [];       // row labels (truncated)
var streamFullRows = [];   // row labels (full worker names)
var streamRowManagers = []; // manager color per row
var streamManagerColors = {}; // manager_id -> color
var memoryPoints = [];     // memory chart points
var memoryScale = "linear";
var memoryYTicks = [];
var streamTicks = [];
var streamWindow = 300;    // seconds
var streamNeedsRedraw = false;
var memoryNeedsRedraw = false;

// ── DOM refs ──
var $ = function(id) { return document.getElementById(id); };
var connStatus = $("conn-status");
var schedAddress = $("sched-address");
var schedCpu = $("sched-cpu");
var schedRss = $("sched-rss");
var schedRssFree = $("sched-rss-free");
var managersBody = $("managers-body");
var workersBody = $("workers-body");
var tasklogBody = $("tasklog-body");
var tasklogCount = $("tasklog-count");
var streamCanvas = $("stream-canvas");
var streamCtx = streamCanvas.getContext("2d");
var streamContainer = $("stream-container");
var streamAxis = $("stream-axis");
var streamLegend = $("stream-legend");
var memoryCanvas = $("memory-canvas");
var memoryCtx = memoryCanvas.getContext("2d");
var processorsContainer = $("processors-container");
var tooltip = $("tooltip");

// ── Tabs ──
var tabs = document.querySelectorAll(".tab");
var panels = document.querySelectorAll(".tab-panel");

for (var i = 0; i < tabs.length; i++) {
    tabs[i].addEventListener("click", (function(tab) {
        return function() {
            for (var j = 0; j < tabs.length; j++) {
                tabs[j].classList.remove("active");
                panels[j].classList.remove("active");
            }
            tab.classList.add("active");
            var panel = $("panel-" + tab.getAttribute("data-tab"));
            if (panel) panel.classList.add("active");
            updateFitPageStream();
            // trigger redraws for canvas tabs
            if (tab.getAttribute("data-tab") === "stream") {
                streamNeedsRedraw = true;
                memoryNeedsRedraw = true;
            }
        };
    })(tabs[i]));
}

// ── Fit Page Toggle ──
var fitPageBtn = $("fit-page-btn");
var fitPageActive = false;

function updateFitPageStream() {
    var streamActive = document.querySelector('.tab.active');
    var isStream = streamActive && streamActive.getAttribute('data-tab') === 'stream';
    document.body.classList.toggle('fit-page-stream', fitPageActive && isStream);
}

fitPageBtn.addEventListener("click", function() {
    fitPageActive = !fitPageActive;
    document.body.classList.toggle("fit-page", fitPageActive);
    fitPageBtn.classList.toggle("active", fitPageActive);
    updateFitPageStream();
    streamNeedsRedraw = true;
    memoryNeedsRedraw = true;
});

// ── Settings ──
function setupToggle(groupId, callback) {
    var group = $(groupId);
    if (!group) return;
    var btns = group.querySelectorAll(".toggle-btn");
    for (var i = 0; i < btns.length; i++) {
        btns[i].addEventListener("click", (function(btn) {
            return function() {
                for (var j = 0; j < btns.length; j++) {
                    btns[j].classList.remove("active");
                }
                btn.classList.add("active");
                callback(btn.getAttribute("data-value"));
            };
        })(btns[i]));
    }
}

setupToggle("window-toggle", function(val) {
    sendSettings({ stream_window: parseInt(val, 10) });
});

setupToggle("scale-toggle", function(val) {
    sendSettings({ memory_scale: val });
});

function sendSettings(settings) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "settings", settings: settings }));
    }
}

// ── WebSocket ──
function connect() {
    var proto = location.protocol === "https:" ? "wss:" : "ws:";
    ws = new WebSocket(proto + "//" + location.host + "/ws");

    ws.onopen = function() {
        connStatus.textContent = "Connected";
        connStatus.classList.add("connected");
        reconnectDelay = 500;
    };

    ws.onclose = function() {
        connStatus.textContent = "Disconnected";
        connStatus.classList.remove("connected");
        setTimeout(connect, Math.min(reconnectDelay, 10000));
        reconnectDelay *= 2;
    };

    ws.onerror = function() {
        ws.close();
    };

    ws.onmessage = function(evt) {
        var data;
        try {
            data = JSON.parse(evt.data);
        } catch (e) {
            return;
        }
        handleMessage(data);
    };
}

function handleMessage(data) {
    if (data.type === "full_state") {
        handleFullState(data);
        return;
    }

    if (data.scheduler) {
        updateScheduler(data.scheduler);
    }
    if (data.workers) {
        updateWorkers(data.workers);
    }
    if (data.worker_managers) {
        updateWorkerManagers(data.worker_managers);
    }
    if (data.worker_events) {
        handleWorkerEvents(data.worker_events);
    }
    if (data.task_updates) {
        handleTaskUpdates(data.task_updates);
    }
    if (data.task_stream) {
        updateTaskStream(data.task_stream);
    }
    if (data.memory_chart) {
        updateMemoryChart(data.memory_chart);
    }
    if (data.processors) {
        updateProcessors(data.processors);
    }
}

function handleFullState(data) {
    if (data.scheduler) updateScheduler(data.scheduler);
    if (data.workers) updateWorkers(data.workers);
    if (data.worker_managers) updateWorkerManagers(data.worker_managers);
    if (data.task_log) {
        tasklogBody.innerHTML = "";
        taskLogCount = 0;
        taskRowMap = {};
        addTaskLogEntries(data.task_log, true);
    }
    if (data.task_stream) updateTaskStream(data.task_stream);
    if (data.memory_chart) updateMemoryChart(data.memory_chart);
    if (data.processors) updateProcessors(data.processors);
    if (data.settings) applySettings(data.settings);
}

function applySettings(settings) {
    if (settings.stream_window) {
        var btns = $("window-toggle").querySelectorAll(".toggle-btn");
        for (var i = 0; i < btns.length; i++) {
            btns[i].classList.toggle("active", btns[i].getAttribute("data-value") === String(settings.stream_window));
        }
    }
    if (settings.memory_scale) {
        var btns2 = $("scale-toggle").querySelectorAll(".toggle-btn");
        for (var i = 0; i < btns2.length; i++) {
            btns2[i].classList.toggle("active", btns2[i].getAttribute("data-value") === settings.memory_scale);
        }
    }
}

// ── Live Tab: Scheduler ──
function updateScheduler(sched) {
    schedAddress.textContent = sched.monitor_address || "—";
    schedCpu.textContent = sched.cpu || "—";
    schedRss.textContent = sched.rss || "—";
    schedRssFree.textContent = sched.rss_free || "—";
}

// ── Live Tab: Worker Managers ──
function updateWorkerManagers(managers) {
    managersBody.innerHTML = "";
    if (!managers || managers.length === 0) {
        var tr = document.createElement("tr");
        var td = document.createElement("td");
        td.colSpan = 12;
        td.style.color = "#64748b";
        td.textContent = "No worker managers connected";
        tr.appendChild(td);
        managersBody.appendChild(tr);
        return;
    }
    for (var i = 0; i < managers.length; i++) {
        var m = managers[i];
        var tr = document.createElement("tr");

        var tdId = document.createElement("td");
        tdId.textContent = m.manager_id || "—";
        tr.appendChild(tdId);

        var tdAddr = document.createElement("td");
        tdAddr.textContent = m.identity || "—";
        tdAddr.title = m.identity || "";
        tr.appendChild(tdAddr);

        var tdSeen = document.createElement("td");
        tdSeen.textContent = m.last_seen || "—";
        tr.appendChild(tdSeen);

        var tdConc = document.createElement("td");
        tdConc.textContent = m.max_task_concurrency != null ? m.max_task_concurrency : "—";
        tr.appendChild(tdConc);

        var tdWC = document.createElement("td");
        tdWC.textContent = m.worker_count != null ? m.worker_count : "0";
        tr.appendChild(tdWC);

        var tdCpu = document.createElement("td");
        tdCpu.textContent = m.total_proc_cpu != null ? m.total_proc_cpu + "%" : "—";
        tr.appendChild(tdCpu);

        var tdRss = document.createElement("td");
        tdRss.textContent = m.total_proc_rss != null ? m.total_proc_rss : "—";
        tr.appendChild(tdRss);

        var tdFree = document.createElement("td");
        tdFree.textContent = m.total_free != null ? m.total_free : "—";
        tr.appendChild(tdFree);

        var tdSent = document.createElement("td");
        tdSent.textContent = m.total_sent != null ? m.total_sent : "—";
        tr.appendChild(tdSent);

        var tdQueued = document.createElement("td");
        tdQueued.textContent = m.total_queued != null ? m.total_queued : "—";
        tr.appendChild(tdQueued);

        var tdSusp = document.createElement("td");
        tdSusp.textContent = m.total_suspended != null ? m.total_suspended : "—";
        tr.appendChild(tdSusp);

        var tdCaps = document.createElement("td");
        tdCaps.textContent = m.capabilities || "—";
        tr.appendChild(tdCaps);

        managersBody.appendChild(tr);
    }
}

// ── Live Tab: Workers ──
var WORKER_FIELDS = ["name", "manager_id", "agt_cpu", "agt_rss", "proc_cpu", "proc_rss",
                     "free", "sent", "queued", "suspended", "lag", "itl", "last_seen", "capabilities"];
var WORKER_NUMERIC_FIELDS = {"agt_cpu":1, "agt_rss":1, "proc_cpu":1, "proc_rss":1,
                             "free":1, "sent":1, "queued":1, "suspended":1, "itl":1};

function updateWorkers(workers) {
    lastWorkersData = workers;
    var seen = {};
    for (var i = 0; i < workers.length; i++) {
        var w = workers[i];
        seen[w.id] = true;
        var row = workerRows[w.id];
        if (!row) {
            row = createWorkerRow(w);
            workerRows[w.id] = row;
            workersBody.appendChild(row);
        }
        updateWorkerRow(row, w);
    }
    // remove dead workers
    var ids = Object.keys(workerRows);
    for (var j = 0; j < ids.length; j++) {
        if (!seen[ids[j]]) {
            workersBody.removeChild(workerRows[ids[j]]);
            delete workerRows[ids[j]];
        }
    }
    // apply current sort order
    if (workerSortField) {
        applySortOrder();
    }
}

function applySortOrder() {
    var rows = Array.prototype.slice.call(workersBody.children);
    var field = workerSortField;
    var asc = workerSortAsc;
    var isNumeric = WORKER_NUMERIC_FIELDS[field];

    // build a lookup from the latest data
    var dataById = {};
    for (var i = 0; i < lastWorkersData.length; i++) {
        dataById[lastWorkersData[i].id] = lastWorkersData[i];
    }

    rows.sort(function(a, b) {
        var wa = dataById[a.getAttribute("data-worker")];
        var wb = dataById[b.getAttribute("data-worker")];
        if (!wa || !wb) return 0;
        var va = wa[field], vb = wb[field];
        if (va == null) va = "";
        if (vb == null) vb = "";
        var cmp;
        if (isNumeric) {
            cmp = (Number(va) || 0) - (Number(vb) || 0);
        } else {
            cmp = String(va).localeCompare(String(vb));
        }
        return asc ? cmp : -cmp;
    });

    for (var j = 0; j < rows.length; j++) {
        workersBody.appendChild(rows[j]);
    }
}

function setupWorkerSort() {
    var thead = workersBody.parentElement.querySelector("thead tr");
    if (!thead) return;
    var ths = thead.children;
    for (var i = 0; i < ths.length; i++) {
        ths[i].classList.add("sortable");
        ths[i].setAttribute("data-sort-field", WORKER_FIELDS[i]);
        (function(th, field) {
            th.addEventListener("click", function() {
                if (workerSortField === field) {
                    workerSortAsc = !workerSortAsc;
                } else {
                    workerSortField = field;
                    workerSortAsc = true;
                }
                // update header indicators
                var allTh = th.parentElement.children;
                for (var k = 0; k < allTh.length; k++) {
                    allTh[k].classList.remove("sort-asc", "sort-desc");
                }
                th.classList.add(workerSortAsc ? "sort-asc" : "sort-desc");
                applySortOrder();
            });
        })(ths[i], WORKER_FIELDS[i]);
    }
}
setupWorkerSort();

function createWorkerRow(w) {
    var tr = document.createElement("tr");
    tr.setAttribute("data-worker", w.id);
    // 14 cells
    var fields = ["name", "manager_id", "agt_cpu", "agt_rss", "proc_cpu", "proc_rss",
                  "free", "sent", "queued", "suspended", "lag", "itl", "last_seen", "capabilities"];
    for (var i = 0; i < fields.length; i++) {
        var td = document.createElement("td");
        td.setAttribute("data-field", fields[i]);
        tr.appendChild(td);
    }
    return tr;
}

function makeGaugeHTML(value, max, unit) {
    if (max <= 0) max = 100;
    var pct = Math.min(100, (value / max) * 100);
    var cls = pct > 90 ? "critical" : pct > 70 ? "high" : "";
    return '<div class="gauge"><div class="gauge-bar"><div class="gauge-fill ' + cls +
        '" style="width:' + pct.toFixed(1) + '%"></div></div><span class="gauge-value">' +
        value + (unit || "") + '</span></div>';
}

function updateWorkerRow(tr, w) {
    var cells = tr.children;
    cells[0].textContent = w.name;
    cells[0].title = w.full_name || w.name;
    cells[1].textContent = w.manager_id || "—";
    cells[2].innerHTML = makeGaugeHTML(w.agt_cpu, 100, "%");
    cells[3].innerHTML = makeGaugeHTML(w.agt_rss, w.total_rss, "");
    cells[4].innerHTML = makeGaugeHTML(w.proc_cpu, 100, "%");
    cells[5].innerHTML = makeGaugeHTML(w.proc_rss, w.total_rss, "");
    cells[6].textContent = w.free;
    cells[7].textContent = w.sent;
    cells[8].textContent = w.queued;
    cells[9].textContent = w.suspended;
    cells[10].textContent = w.lag;
    cells[11].textContent = w.itl;
    cells[12].textContent = w.last_seen;
    cells[13].textContent = w.capabilities;
}

function handleWorkerEvents(events) {
    for (var i = 0; i < events.length; i++) {
        var ev = events[i];
        if (ev.state === "Disconnected" && workerRows[ev.worker_id]) {
            workersBody.removeChild(workerRows[ev.worker_id]);
            delete workerRows[ev.worker_id];
        }
    }
}

// ── Task Log ──
function formatTime(epoch) {
    if (!epoch) return "";
    var d = new Date(epoch * 1000);
    var h = String(d.getHours()).padStart(2, "0");
    var m = String(d.getMinutes()).padStart(2, "0");
    var s = String(d.getSeconds()).padStart(2, "0");
    return h + ":" + m + ":" + s;
}

function statusClass(status) {
    if (status === "Success") return "status-success";
    if (status === "Running" || status === "Inactive" || status === "Canceling" || status === "BalanceCanceling") return "status-running";
    return "status-fail";
}

function handleTaskUpdates(entries) {
    for (var i = 0; i < entries.length; i++) {
        var e = entries[i];
        var existing = taskRowMap[e.task_id];
        if (existing) {
            // update cells in-place: worker(2), time(3), duration(4), peak_mem(5), status(6)
            var cells = existing.children;
            cells[2].textContent = e.worker || "";
            cells[2].title = e.full_worker || e.worker || "";
            cells[3].textContent = formatTime(e.time);
            cells[4].textContent = e.duration;
            cells[5].textContent = e.peak_mem;
            cells[6].textContent = e.status;
            cells[6].className = statusClass(e.status);
        } else {
            // new task - insert at top
            addTaskLogEntries([e]);
        }
    }
}

function addTaskLogEntries(entries, append) {
    for (var i = 0; i < entries.length; i++) {
        var e = entries[i];
        var tr = document.createElement("tr");
        tr.dataset.taskId = e.task_id;

        // Task ID (clickable to copy)
        var tdId = document.createElement("td");
        var span = document.createElement("span");
        span.className = "task-id";
        span.textContent = e.task_id;
        span.title = e.task_id;
        span.addEventListener("click", (function(id) {
            return function() {
                if (navigator.clipboard) {
                    navigator.clipboard.writeText(id);
                }
            };
        })(e.task_id));
        tdId.appendChild(span);
        tr.appendChild(tdId);

        // Function
        var tdFunc = document.createElement("td");
        tdFunc.textContent = e.function;
        tr.appendChild(tdFunc);

        // Worker
        var tdWorker = document.createElement("td");
        tdWorker.textContent = e.worker || "";
        tdWorker.title = e.full_worker || e.worker || "";
        tr.appendChild(tdWorker);

        // Time
        var tdTime = document.createElement("td");
        tdTime.textContent = formatTime(e.time);
        tr.appendChild(tdTime);

        // Duration
        var tdDur = document.createElement("td");
        tdDur.textContent = e.duration;
        tr.appendChild(tdDur);

        // Peak Mem
        var tdMem = document.createElement("td");
        tdMem.textContent = e.peak_mem;
        tr.appendChild(tdMem);

        // Status
        var tdStatus = document.createElement("td");
        tdStatus.textContent = e.status;
        tdStatus.className = statusClass(e.status);
        tr.appendChild(tdStatus);

        // Capabilities
        var tdCaps = document.createElement("td");
        tdCaps.textContent = e.capabilities;
        tr.appendChild(tdCaps);

        // Insert row
        if (append) {
            tasklogBody.appendChild(tr);
        } else if (tasklogBody.firstChild) {
            tasklogBody.insertBefore(tr, tasklogBody.firstChild);
        } else {
            tasklogBody.appendChild(tr);
        }
        taskRowMap[e.task_id] = tr;
        taskLogCount++;
    }

    // Trim to configured size
    while (tasklogBody.children.length > TASK_LOG_MAX_SIZE) {
        var removed = tasklogBody.lastChild;
        if (removed && removed.dataset && removed.dataset.taskId) {
            delete taskRowMap[removed.dataset.taskId];
        }
        tasklogBody.removeChild(removed);
        taskLogCount--;
    }
    tasklogCount.textContent = Math.min(taskLogCount, TASK_LOG_MAX_SIZE);
}

// ── Task Stream (Canvas) ──
var STREAM_LABEL_WIDTH = 120;
var STREAM_ROW_HEIGHT = 24;
var STREAM_PADDING_TOP = 4;

function updateTaskStream(data) {
    streamBars = data.bars || [];
    streamRows = data.rows || [];
    streamFullRows = data.full_rows || streamRows;
    streamRowManagers = data.row_managers || [];
    streamManagerColors = {};
    var managerLegend = data.manager_legend || [];
    for (var ml = 0; ml < managerLegend.length; ml++) {
        streamManagerColors[managerLegend[ml].name] = managerLegend[ml].color;
    }
    streamTicks = data.ticks || [];
    streamWindow = data.window || 300;
    streamNeedsRedraw = true;

    // Update legend
    var legend = data.legend || [];
    var managerLegend = data.manager_legend || [];
    streamLegend.innerHTML = "";
    // Add status patterns to legend
    var failed = document.createElement("span");
    failed.className = "legend-item";
    failed.innerHTML = '<span class="legend-swatch pattern-x"></span> Failed';
    streamLegend.appendChild(failed);

    var canceled = document.createElement("span");
    canceled.className = "legend-item";
    canceled.innerHTML = '<span class="legend-swatch pattern-slash"></span> Canceled';
    streamLegend.appendChild(canceled);

    // Manager legend first (with separator)
    if (managerLegend.length > 0) {
        var sep1 = document.createElement("span");
        sep1.className = "legend-item";
        sep1.style.color = "#94a3b8";
        sep1.textContent = "|";
        streamLegend.appendChild(sep1);
        for (var k = 0; k < managerLegend.length; k++) {
            var mItem = document.createElement("span");
            mItem.className = "legend-item";
            mItem.innerHTML = '<span class="legend-swatch" style="background:' +
                managerLegend[k].color + '"></span> ' + escapeHTML(managerLegend[k].name);
            streamLegend.appendChild(mItem);
        }
    }

    // Capability legend (with separator)
    if (legend.length > 0) {
        var sep2 = document.createElement("span");
        sep2.className = "legend-item";
        sep2.style.color = "#94a3b8";
        sep2.textContent = "|";
        streamLegend.appendChild(sep2);
    }
    for (var i = 0; i < legend.length; i++) {
        var item = document.createElement("span");
        item.className = "legend-item";
        item.innerHTML = '<span class="legend-swatch" style="background:' + legend[i].color + '"></span> ' +
            escapeHTML(legend[i].name);
        streamLegend.appendChild(item);
    }

    // Update axis
    streamAxis.innerHTML = "";
    streamAxis.style.paddingLeft = STREAM_LABEL_WIDTH + "px";
    for (var j = 0; j < streamTicks.length; j++) {
        var tick = document.createElement("span");
        tick.textContent = streamTicks[j].label;
        streamAxis.appendChild(tick);
    }
}

function drawTaskStream() {
    var dpr = window.devicePixelRatio || 1;
    var containerWidth = streamContainer.clientWidth;
    var chartWidth = containerWidth - STREAM_LABEL_WIDTH;
    var numRows = streamRows.length;
    var canvasHeight = STREAM_PADDING_TOP + numRows * STREAM_ROW_HEIGHT + 4;

    streamCanvas.width = containerWidth * dpr;
    streamCanvas.height = canvasHeight * dpr;
    streamCanvas.style.width = containerWidth + "px";
    streamCanvas.style.height = canvasHeight + "px";
    streamCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // Clear
    streamCtx.fillStyle = "#ffffff";
    streamCtx.fillRect(0, 0, containerWidth, canvasHeight);

    // Draw row labels and grid lines
    streamCtx.font = "11px " + getComputedStyle(document.body).fontFamily;
    streamCtx.textBaseline = "middle";
    for (var i = 0; i < numRows; i++) {
        var y = STREAM_PADDING_TOP + i * STREAM_ROW_HEIGHT;
        // alternating row bg
        if (i % 2 === 0) {
            streamCtx.fillStyle = "#f8fafc";
            streamCtx.fillRect(0, y, containerWidth, STREAM_ROW_HEIGHT);
        }
        // grid line
        streamCtx.strokeStyle = "#e2e8f0";
        streamCtx.beginPath();
        streamCtx.moveTo(STREAM_LABEL_WIDTH, y + STREAM_ROW_HEIGHT);
        streamCtx.lineTo(containerWidth, y + STREAM_ROW_HEIGHT);
        streamCtx.stroke();
        // label
        streamCtx.fillStyle = "#334155";
        streamCtx.fillText(streamRows[i], 4, y + STREAM_ROW_HEIGHT / 2);
        // manager color stripe
        var mgr = streamRowManagers[i];
        if (mgr && streamManagerColors[mgr]) {
            streamCtx.fillStyle = streamManagerColors[mgr];
            streamCtx.fillRect(0, y, 4, STREAM_ROW_HEIGHT);
        }
    }

    // Draw bars: two passes so outlines are always visible between adjacent bars
    // Pass 1: fills, patterns, and outlines for cancelled bars (so they stay beneath completed bars)
    for (var j = 0; j < streamBars.length; j++) {
        var bar = streamBars[j];
        var fullBarHeight = STREAM_ROW_HEIGHT - 4;
        var barHeight = bar.p === "/" ? Math.floor(fullBarHeight / 2) : fullBarHeight;
        var rowY = STREAM_PADDING_TOP + bar.r * STREAM_ROW_HEIGHT + 2 + (fullBarHeight - barHeight);
        var x1 = STREAM_LABEL_WIDTH + ((bar.x + streamWindow) / streamWindow) * chartWidth;
        var x2 = STREAM_LABEL_WIDTH + ((bar.x + bar.w + streamWindow) / streamWindow) * chartWidth;
        var barWidth = Math.max(x2 - x1, 1);

        var colors = bar.cs;
        if (colors.length === 1) {
            streamCtx.fillStyle = colors[0];
            streamCtx.fillRect(x1, rowY, barWidth, barHeight);
        } else {
            // cyclic vertical stripes, 6px each, not squished
            var stripeW = 6;
            var cx = 0;
            var ci = 0;
            while (cx < barWidth) {
                var sw = Math.min(stripeW, barWidth - cx);
                streamCtx.fillStyle = colors[ci % colors.length];
                streamCtx.fillRect(x1 + cx, rowY, sw, barHeight);
                cx += sw;
                ci++;
            }
        }

        if (bar.p === "x") {
            drawCrossHatch(streamCtx, x1, rowY, barWidth, barHeight);
        } else if (bar.p === "/") {
            drawSlashHatch(streamCtx, x1, rowY, barWidth, barHeight);
            // draw outline in same layer so completed bars paint over it
            if (bar.ow > 0) {
                streamCtx.strokeStyle = bar.oc;
                streamCtx.lineWidth = bar.ow;
                streamCtx.strokeRect(x1, rowY, barWidth, barHeight);
            }
        }
    }

    // Pass 2: outlines on top (skip cancelled bars — already drawn in pass 1)
    for (var j = 0; j < streamBars.length; j++) {
        var bar = streamBars[j];
        if (bar.ow > 0 && bar.p !== "/") {
            var fullBarHeight = STREAM_ROW_HEIGHT - 4;
            var barHeight = fullBarHeight;
            var rowY = STREAM_PADDING_TOP + bar.r * STREAM_ROW_HEIGHT + 2;
            var x1 = STREAM_LABEL_WIDTH + ((bar.x + streamWindow) / streamWindow) * chartWidth;
            var x2 = STREAM_LABEL_WIDTH + ((bar.x + bar.w + streamWindow) / streamWindow) * chartWidth;
            var barWidth = Math.max(x2 - x1, 1);
            streamCtx.strokeStyle = bar.oc;
            streamCtx.lineWidth = bar.ow;
            streamCtx.strokeRect(x1, rowY, barWidth, barHeight);
        }
    }

    streamCtx.lineWidth = 1;
}

function drawCrossHatch(ctx, x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.lineWidth = 1;
    var step = 6;
    for (var i = -h; i < w + h; i += step) {
        ctx.beginPath();
        ctx.moveTo(x + i, y);
        ctx.lineTo(x + i + h, y + h);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(x + i + h, y);
        ctx.lineTo(x + i, y + h);
        ctx.stroke();
    }
    ctx.restore();
}

function drawSlashHatch(ctx, x, y, w, h) {
    ctx.save();
    ctx.beginPath();
    ctx.rect(x, y, w, h);
    ctx.clip();
    ctx.strokeStyle = "rgba(0,0,0,0.5)";
    ctx.lineWidth = 1;
    var step = 6;
    for (var i = -h; i < w + h; i += step) {
        ctx.beginPath();
        ctx.moveTo(x + i + h, y);
        ctx.lineTo(x + i, y + h);
        ctx.stroke();
    }
    ctx.restore();
}

// Stream hover tooltip
streamCanvas.addEventListener("mousemove", function(evt) {
    var rect = streamCanvas.getBoundingClientRect();
    var mx = evt.clientX - rect.left;
    var my = evt.clientY - rect.top;

    var containerWidth = streamContainer.clientWidth;
    var chartWidth = containerWidth - STREAM_LABEL_WIDTH;

    for (var i = streamBars.length - 1; i >= 0; i--) {
        var bar = streamBars[i];
        var fullBarHeight = STREAM_ROW_HEIGHT - 4;
        var barHeight = bar.p === "/" ? Math.floor(fullBarHeight / 2) : fullBarHeight;
        var rowY = STREAM_PADDING_TOP + bar.r * STREAM_ROW_HEIGHT + 2 + (fullBarHeight - barHeight);
        var x1 = STREAM_LABEL_WIDTH + ((bar.x + streamWindow) / streamWindow) * chartWidth;
        var x2 = STREAM_LABEL_WIDTH + ((bar.x + bar.w + streamWindow) / streamWindow) * chartWidth;

        if (mx >= x1 && mx <= x2 && my >= rowY && my <= rowY + barHeight) {
            tooltip.textContent = bar.h;
            tooltip.style.left = (evt.clientX + 10) + "px";
            tooltip.style.top = (evt.clientY - 30) + "px";
            tooltip.classList.add("visible");
            return;
        }
    }

    // If not over a bar, check if hovering over a row label
    if (mx < STREAM_LABEL_WIDTH) {
        for (var r = 0; r < streamFullRows.length; r++) {
            var ry = STREAM_PADDING_TOP + r * STREAM_ROW_HEIGHT;
            if (my >= ry && my < ry + STREAM_ROW_HEIGHT) {
                streamCanvas.title = streamFullRows[r];
                tooltip.classList.remove("visible");
                return;
            }
        }
    }

    streamCanvas.title = "";
    tooltip.classList.remove("visible");
});

streamCanvas.addEventListener("mouseleave", function() {
    tooltip.classList.remove("visible");
});

// ── Memory Chart (Canvas) ──
var MEM_LABEL_WIDTH = 80;
var MEM_PADDING = { top: 20, right: 20, bottom: 30, left: MEM_LABEL_WIDTH };

function updateMemoryChart(data) {
    memoryPoints = data.points || [];
    memoryYTicks = data.y_ticks || [];
    memoryScale = data.scale || "linear";
    streamWindow = data.window || streamWindow;
    memoryNeedsRedraw = true;
}

function drawMemoryChart() {
    var container = memoryCanvas.parentElement;
    var dpr = window.devicePixelRatio || 1;
    var cw = container.clientWidth;
    var ch = container.clientHeight;

    memoryCanvas.width = cw * dpr;
    memoryCanvas.height = ch * dpr;
    memoryCanvas.style.width = cw + "px";
    memoryCanvas.style.height = ch + "px";
    memoryCtx.setTransform(dpr, 0, 0, dpr, 0, 0);

    var plotLeft = MEM_PADDING.left;
    var plotTop = MEM_PADDING.top;
    var plotWidth = cw - MEM_PADDING.left - MEM_PADDING.right;
    var plotHeight = ch - MEM_PADDING.top - MEM_PADDING.bottom;

    // Clear
    memoryCtx.fillStyle = "#ffffff";
    memoryCtx.fillRect(0, 0, cw, ch);

    if (memoryPoints.length === 0) {
        memoryCtx.fillStyle = "#94a3b8";
        memoryCtx.font = "13px " + getComputedStyle(document.body).fontFamily;
        memoryCtx.textAlign = "center";
        memoryCtx.fillText("No memory data", cw / 2, ch / 2);
        return;
    }

    // Determine y range
    var maxY = 0;
    for (var i = 0; i < memoryPoints.length; i++) {
        if (memoryPoints[i].y > maxY) maxY = memoryPoints[i].y;
    }
    maxY = Math.max(maxY, 1024 * 1024 * 1024); // min 1GB

    function mapX(val) {
        return plotLeft + ((val + streamWindow) / streamWindow) * plotWidth;
    }

    function mapY(val) {
        if (memoryScale === "log") {
            if (val <= 0) return plotTop + plotHeight;
            var logMax = Math.log10(maxY);
            var logVal = Math.log10(Math.max(val, 1));
            return plotTop + plotHeight - (logVal / logMax) * plotHeight;
        }
        return plotTop + plotHeight - (val / maxY) * plotHeight;
    }

    // Grid lines
    memoryCtx.strokeStyle = "#e2e8f0";
    memoryCtx.lineWidth = 1;
    memoryCtx.font = "10px " + getComputedStyle(document.body).fontFamily;
    memoryCtx.textAlign = "right";
    memoryCtx.textBaseline = "middle";
    memoryCtx.fillStyle = "#64748b";

    for (var t = 0; t < memoryYTicks.length; t++) {
        var ty = mapY(memoryYTicks[t].val);
        memoryCtx.beginPath();
        memoryCtx.moveTo(plotLeft, ty);
        memoryCtx.lineTo(plotLeft + plotWidth, ty);
        memoryCtx.stroke();
        memoryCtx.fillText(memoryYTicks[t].label, plotLeft - 6, ty);
    }

    // X axis ticks
    memoryCtx.textAlign = "center";
    memoryCtx.textBaseline = "top";
    for (var s = 0; s < streamTicks.length; s++) {
        var tx = mapX(streamTicks[s].val);
        memoryCtx.beginPath();
        memoryCtx.moveTo(tx, plotTop);
        memoryCtx.lineTo(tx, plotTop + plotHeight);
        memoryCtx.stroke();
        memoryCtx.fillText(streamTicks[s].label, tx, plotTop + plotHeight + 4);
    }

    // Draw filled area
    memoryCtx.beginPath();
    memoryCtx.moveTo(mapX(memoryPoints[0].x), mapY(0));
    for (var p = 0; p < memoryPoints.length; p++) {
        memoryCtx.lineTo(mapX(memoryPoints[p].x), mapY(memoryPoints[p].y));
    }
    memoryCtx.lineTo(mapX(memoryPoints[memoryPoints.length - 1].x), mapY(0));
    memoryCtx.closePath();
    memoryCtx.fillStyle = "rgba(59, 130, 246, 0.3)";
    memoryCtx.fill();

    // Draw line
    memoryCtx.beginPath();
    for (var q = 0; q < memoryPoints.length; q++) {
        var px = mapX(memoryPoints[q].x);
        var py = mapY(memoryPoints[q].y);
        if (q === 0) memoryCtx.moveTo(px, py);
        else memoryCtx.lineTo(px, py);
    }
    memoryCtx.strokeStyle = "#3b82f6";
    memoryCtx.lineWidth = 2;
    memoryCtx.stroke();

    memoryCtx.lineWidth = 1;
}

// Memory hover
memoryCanvas.addEventListener("mousemove", function(evt) {
    if (memoryPoints.length === 0) return;
    var rect = memoryCanvas.getBoundingClientRect();
    var mx = evt.clientX - rect.left;
    var container = memoryCanvas.parentElement;
    var cw = container.clientWidth;
    var plotWidth = cw - MEM_PADDING.left - MEM_PADDING.right;

    // convert mx to time
    var t = ((mx - MEM_PADDING.left) / plotWidth) * streamWindow - streamWindow;

    // find closest point
    var closest = null;
    var minDist = Infinity;
    for (var i = 0; i < memoryPoints.length; i++) {
        var d = Math.abs(memoryPoints[i].x - t);
        if (d < minDist) {
            minDist = d;
            closest = memoryPoints[i];
        }
    }

    if (closest && minDist < streamWindow * 0.05) {
        tooltip.textContent = formatBytes(closest.y) + " at " + closest.x.toFixed(1) + "s";
        tooltip.style.left = (evt.clientX + 10) + "px";
        tooltip.style.top = (evt.clientY - 30) + "px";
        tooltip.classList.add("visible");
    } else {
        tooltip.classList.remove("visible");
    }
});

memoryCanvas.addEventListener("mouseleave", function() {
    tooltip.classList.remove("visible");
});

// ── Worker Processors ──
var processorsCollapsed = {};  // track collapsed state by worker name
var managerCollapsed = {};    // track collapsed state by manager id

function updateProcessors(processors) {
    processorsContainer.innerHTML = "";
    if (!processors || processors.length === 0) {
        processorsContainer.innerHTML = '<div class="card"><p style="color:#64748b">No workers connected</p></div>';
        return;
    }

    for (var g = 0; g < processors.length; g++) {
        var group = processors[g];

        // Manager group as collapsible details/summary
        var managerSection = document.createElement("details");
        managerSection.className = "manager-group card";
        managerSection.open = !managerCollapsed[group.manager_id];

        var managerSummary = document.createElement("summary");
        managerSummary.className = "manager-header";
        managerSummary.innerHTML =
            '<span class="manager-title">Manager: ' + escapeHTML(group.manager_id) + '</span>' +
            '<span class="manager-stats">' +
                '<span class="manager-stat"><b>Workers:</b> ' + group.worker_count + '</span>' +
                '<span class="manager-stat"><b>Processors:</b> ' + group.active_processors + ' active</span>' +
                '<span class="manager-stat"><b>Total RSS:</b> ' + group.total_rss + ' MB</span>' +
                '<span class="manager-stat"><b>RSS Free:</b> ' + group.total_rss_free + ' MB</span>' +
                '<span class="manager-stat"><b>Total CPU:</b> ' + group.total_cpu + '%</span>' +
            '</span>';
        managerSection.appendChild(managerSummary);

        // track manager toggle state
        (function(mid, el) {
            el.addEventListener("toggle", function() {
                managerCollapsed[mid] = !el.open;
            });
        })(group.manager_id, managerSection);

        // Worker details within this manager group
        var workers = group.workers;
        if (workers.length === 0) {
            var emptyMsg = document.createElement("p");
            emptyMsg.style.color = "#64748b";
            emptyMsg.style.padding = "8px 16px";
            emptyMsg.textContent = "No workers currently running for this manager";
            managerSection.appendChild(emptyMsg);
        }
        for (var i = 0; i < workers.length; i++) {
            var wp = workers[i];
            var details = document.createElement("details");
            details.className = "card processor-group";
            details.open = !processorsCollapsed[wp.name];

            var summary = document.createElement("summary");
            summary.textContent = "Worker " + wp.name;
            summary.title = wp.full_name || wp.name;
            details.appendChild(summary);

            // track toggle state
            (function(name, el) {
                el.addEventListener("toggle", function() {
                    processorsCollapsed[name] = !el.open;
                });
            })(wp.name, details);

            var table = document.createElement("table");
            table.className = "data-table";

            // Header
            var thead = document.createElement("thead");
            var headerRow = document.createElement("tr");
            var headers = ["PID", "CPU %", "RSS (MB)", "Max RSS (MB)", "Initialized", "Has Task", "Suspended"];
            for (var h = 0; h < headers.length; h++) {
                var th = document.createElement("th");
                th.textContent = headers[h];
                headerRow.appendChild(th);
            }
            thead.appendChild(headerRow);
            table.appendChild(thead);

            // Body
            var tbody = document.createElement("tbody");
            for (var p = 0; p < wp.processors.length; p++) {
                var proc = wp.processors[p];
                var tr = document.createElement("tr");

                var tdPid = document.createElement("td");
                tdPid.textContent = proc.pid;
                tr.appendChild(tdPid);

                var tdCpu = document.createElement("td");
                tdCpu.innerHTML = makeGaugeHTML(proc.cpu, 100, "%");
                tr.appendChild(tdCpu);

                var tdRss = document.createElement("td");
                tdRss.innerHTML = makeGaugeHTML(proc.rss, proc.rss_max_gauge, "");
                tr.appendChild(tdRss);

                var tdMaxRss = document.createElement("td");
                tdMaxRss.innerHTML = makeGaugeHTML(proc.max_rss, proc.rss_max_gauge, "");
                tr.appendChild(tdMaxRss);

                var tdInit = document.createElement("td");
                tdInit.innerHTML = boolIndicator(proc.initialized);
                tr.appendChild(tdInit);

                var tdTask = document.createElement("td");
                tdTask.innerHTML = boolIndicator(proc.has_task);
                tr.appendChild(tdTask);

                var tdSusp = document.createElement("td");
                tdSusp.innerHTML = boolIndicator(proc.suspended);
                tr.appendChild(tdSusp);

                tbody.appendChild(tr);
            }
            table.appendChild(tbody);
            details.appendChild(table);
            managerSection.appendChild(details);
        }
        processorsContainer.appendChild(managerSection);
    }
}

function boolIndicator(val) {
    return '<span class="bool-indicator ' + (val ? "bool-true" : "bool-false") + '"></span>';
}

// ── Utilities ──
function escapeHTML(str) {
    var div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (bytes === 0) return "0B";
    var units = ["B", "K", "M", "G", "T"];
    var mod = 1024;
    for (var i = 0; i < units.length; i++) {
        if (bytes < mod) {
            if (i < 2) return Math.round(bytes) + units[i];
            return bytes.toFixed(1) + units[i];
        }
        bytes /= mod;
    }
    return bytes.toFixed(1) + "T";
}

// ── Animation Loop ──
function renderLoop() {
    if (streamNeedsRedraw) {
        streamNeedsRedraw = false;
        drawTaskStream();
    }
    if (memoryNeedsRedraw) {
        memoryNeedsRedraw = false;
        drawMemoryChart();
    }
    requestAnimationFrame(renderLoop);
}

// ── Resize handling ──
window.addEventListener("resize", function() {
    streamNeedsRedraw = true;
    memoryNeedsRedraw = true;
});

// ── Start ──
connect();
requestAnimationFrame(renderLoop);
