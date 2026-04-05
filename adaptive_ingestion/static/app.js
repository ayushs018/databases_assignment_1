/* ═══════════════════════════════════════════════════════════════
   SADF Dashboard — Frontend Logic
   Fetch-based API · Dynamic rendering · CRUD · Query · ACID
   ═══════════════════════════════════════════════════════════════ */

// ── State ────────────────────────────────────────────────────
let currentEntity = null;
let currentData = [];
let entityMeta = {};
let editingId = null;
let editingEntity = null;

// ── Boot ─────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
    await loadSession();
    await loadEntities();
    wireEvents();
});

// ── Session ──────────────────────────────────────────────────
async function loadSession() {
    const info = await api("/api/session");
    document.getElementById("session-subtitle").textContent =
        `${info.project_name} — Session ${info.session_id}`;
    document.getElementById("session-badge").textContent = `ID: ${info.session_id}`;
}

// ── Entities ─────────────────────────────────────────────────
// ── Entities ─────────────────────────────────────────────────
async function loadEntities() {
    entityMeta = await api("/api/entities");
    const ul = document.getElementById("entity-list");
    ul.innerHTML = "";
    for (const [name, info] of Object.entries(entityMeta)) {
        const li = document.createElement("li");
        li.textContent = capitalise(name);
        li.title = info.description;
        li.dataset.entity = name;
        li.addEventListener("click", () => selectEntity(name));
        ul.appendChild(li);
    }
}

async function selectEntity(name) {
    currentEntity = name;
    document.getElementById("global-search").value = ""; // Clear global search on tab change
    document.querySelectorAll("#entity-list li").forEach(li =>
        li.classList.toggle("active", li.dataset.entity === name));
    document.getElementById("entity-title").textContent = capitalise(name);
    document.getElementById("empty-state").classList.add("hidden");
    document.getElementById("query-panel").classList.add("hidden");
    document.getElementById("acid-panel").classList.add("hidden");
    document.getElementById("data-view").classList.remove("hidden");
    await refreshTable();
}

// ── Table rendering ──────────────────────────────────────────
async function refreshTable() {
    const entity = currentEntity;
    if (!entity) return;
    try {
        const res = await api(`/api/data/${entity}`);
        // RACE CONDITION GUARD: Only render if we are still on the same entity
        if (entity !== currentEntity) return;
        currentData = res.data || [];
        renderTable(currentData);
    } catch (e) {
        toast("Failed to load data: " + e.message, "error");
    }
}

function renderTable(rows, customCols = null) {
    const thead = document.getElementById("table-head");
    const tbody = document.getElementById("table-body");

    // Determine columns: use provided customCols (global search), 
    // or metadata for current entity, or fallback to data keys.
    let cols = [];
    if (customCols && customCols.length) {
        cols = customCols;
    } else if (currentEntity && entityMeta[currentEntity]) {
        cols = entityMeta[currentEntity].fields;
    } else if (rows.length) {
        cols = Object.keys(rows[0]).filter(k => k !== "__entity");
    }

    if (!rows.length) {
        thead.innerHTML = "";
        tbody.innerHTML = `<tr><td colspan="99" style="text-align:center;padding:2rem;color:var(--text-dim)">No records found</td></tr>`;
        return;
    }

    thead.innerHTML = cols.map(c => `<th>${c}</th>`).join("") + `<th style="width:80px">Actions</th>`;
    tbody.innerHTML = rows.map(r => {
        // Find a suitable ID for actions
        const rid = r.record_id || r.user_id || r.sensor_id || "";
        const entityOfRow = r.__entity || currentEntity;

        const cells = cols.map(c => {
            let val = r[c];
            if (typeof val === "object" && val !== null) val = JSON.stringify(val);
            return `<td title="${escapeHtml(String(val ?? ""))}">${escapeHtml(String(val ?? ""))}</td>`;
        }).join("");

        return `<tr>
            ${cells}
            <td class="cell-actions">
                <button title="Edit" onclick="openEditModal('${escapeHtml(rid)}', '${escapeHtml(entityOfRow)}')">✎</button>
                <button title="Delete" class="delete" onclick="deleteRecord('${escapeHtml(rid)}', '${escapeHtml(entityOfRow)}')">✕</button>
            </td>
        </tr>`;
    }).join("");
}

// ── Search / Filter ──────────────────────────────────────────
document.getElementById("search-input").addEventListener("input", e => {
    const q = e.target.value.toLowerCase();
    if (!q) return renderTable(currentData);
    const filtered = currentData.filter(r =>
        Object.values(r).some(v => String(v).toLowerCase().includes(q))
    );
    renderTable(filtered);
});

// Global Search (Natural Query)
document.getElementById("global-search").addEventListener("input", debounce(async e => {
    const q = e.target.value;
    if (!q) {
        if (currentEntity) selectEntity(currentEntity);
        else {
            document.getElementById("entity-title").textContent = "Select an entity";
            document.getElementById("empty-state").classList.remove("hidden");
            document.getElementById("data-view").classList.add("hidden");
        }
        return;
    }

    const searchToken = q;
    currentEntity = null; // Enter global search mode

    // UI Feedback
    document.querySelectorAll("#entity-list li").forEach(li => li.classList.remove("active"));
    document.getElementById("entity-title").textContent = `Search Results: "${q}"`;
    document.getElementById("data-view").classList.remove("hidden");
    document.getElementById("query-panel").classList.add("hidden");
    document.getElementById("acid-panel").classList.add("hidden");

    try {
        const res = await api(`/api/search?q=${encodeURIComponent(q)}`);
        // If user changed their mind or typed more, abort this stale render
        if (document.getElementById("global-search").value !== searchToken) return;

        currentData = res.data || [];
        const allKeys = new Set();
        currentData.forEach(r => Object.keys(r).forEach(k => {
            if (k !== "__entity") allKeys.add(k);
        }));
        renderTable(currentData, Array.from(allKeys));
    } catch (e) {
        toast("Search failed: " + e.message, "error");
    }
}, 300));

function debounce(fn, ms) {
    let t; return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

// ── CRUD: Create / Edit Modal ────────────────────────────────
function wireEvents() {
    document.getElementById("btn-create").addEventListener("click", () => openCreateModal());
    document.getElementById("modal-close").addEventListener("click", closeModal);
    document.getElementById("modal-cancel").addEventListener("click", closeModal);
    document.getElementById("modal-save").addEventListener("click", saveRecord);
    document.getElementById("modal-overlay").addEventListener("click", e => {
        if (e.target === e.currentTarget) closeModal();
    });

    // Panels
    document.getElementById("btn-query-panel").addEventListener("click", () => togglePanel("query-panel"));
    document.getElementById("btn-acid-panel").addEventListener("click", () => togglePanel("acid-panel"));
    document.getElementById("btn-run-query").addEventListener("click", runQuery);

    // Metadata
    document.getElementById("btn-metadata").addEventListener("click", showMetadata);
    document.getElementById("meta-close").addEventListener("click", () =>
        document.getElementById("meta-overlay").classList.add("hidden"));
    document.getElementById("meta-overlay").addEventListener("click", e => {
        if (e.target === e.currentTarget) e.currentTarget.classList.add("hidden");
    });

    // Close panel buttons
    document.querySelectorAll(".close-panel").forEach(btn =>
        btn.addEventListener("click", () => {
            document.getElementById(btn.dataset.target).classList.add("hidden");
            document.getElementById("data-view").classList.remove("hidden");
        })
    );
}

function openCreateModal() {
    if (!currentEntity) return toast("Select an entity first", "error");
    editingId = null;
    document.getElementById("modal-title").textContent = `New ${capitalise(currentEntity)} record`;
    buildForm({});
    document.getElementById("modal-overlay").classList.remove("hidden");
}

function openEditModal(rid, entity = null) {
    const targetEntity = entity || currentEntity;
    if (!targetEntity) return;
    const record = currentData.find(r => (r.record_id || r.user_id || r.sensor_id) == rid);
    if (!record) return toast("Record not found", "error");
    editingId = rid;
    editingEntity = targetEntity;
    document.getElementById("modal-title").textContent = `Edit ${capitalise(targetEntity)}`;
    buildForm(record, targetEntity);
    document.getElementById("modal-overlay").classList.remove("hidden");
}

function buildForm(record, entity = null) {
    const targetEntity = entity || currentEntity;
    const form = document.getElementById("crud-form");
    let cols = [];
    if (targetEntity && entityMeta[targetEntity]) {
        cols = entityMeta[targetEntity].fields;
    } else {
        cols = ["record_id"];
    }
    form.innerHTML = cols.map(c => {
        const val = record[c] ?? "";
        const displayVal = typeof val === "object" ? JSON.stringify(val) : val;
        const readonly = (c === "reading_id" || c === "user_id" || c === "sensor_id" || c === "id") && editingId;
        return `
            <div>
                <label>${c}</label>
                <input type="text" name="${c}" value="${escapeHtml(String(displayVal))}" ${readonly ? "readonly" : ""}>
            </div>
        `;
    }).join("");
}

function closeModal() {
    document.getElementById("modal-overlay").classList.add("hidden");
    editingId = null;
}

async function saveRecord() {
    const form = document.getElementById("crud-form");
    const data = {};
    form.querySelectorAll("input").forEach(inp => {
        let val = inp.value;
        // Try parse JSON for object fields
        try { const parsed = JSON.parse(val); if (typeof parsed === "object") val = parsed; } catch { }
        // Try parse number
        if (typeof val === "string" && val !== "" && !isNaN(val) && val.trim() !== "") {
            val = Number(val);
        }
        data[inp.name] = val;
    });

    try {
        const targetEntity = editingEntity || currentEntity;
        if (editingId) {
            await api(`/api/data/${targetEntity}/${editingId}`, "PUT", data);
            toast("Record updated", "success");
        } else {
            await api(`/api/data/${targetEntity}`, "POST", data);
            toast("Record created", "success");
        }
        closeModal();
        await refreshTable();
    } catch (e) {
        toast(e.message, "error");
    }
}

async function deleteRecord(rid, entity = null) {
    const targetEntity = entity || currentEntity;
    if (!confirm(`Delete record ${rid} from ${targetEntity}?`)) return;
    try {
        await api(`/api/data/${targetEntity}/${rid}`, "DELETE");
        toast("Record deleted", "success");
        await refreshTable();
    } catch (e) {
        toast(e.message, "error");
    }
}

// ── Panels ───────────────────────────────────────────────────
function togglePanel(panelId) {
    const panel = document.getElementById(panelId);
    const visible = !panel.classList.contains("hidden");
    document.getElementById("data-view").classList.toggle("hidden", !visible);
    document.getElementById("query-panel").classList.add("hidden");
    document.getElementById("acid-panel").classList.add("hidden");
    if (!visible) {
        panel.classList.remove("hidden");
        document.getElementById("data-view").classList.add("hidden");
    }
}

// ── JSON Query ───────────────────────────────────────────────
async function runQuery() {
    const editor = document.getElementById("query-editor");
    const resultEl = document.getElementById("query-result");
    let body;
    try { body = JSON.parse(editor.value); } catch {
        resultEl.textContent = "❌ Invalid JSON";
        resultEl.style.color = "var(--red)";
        return;
    }
    try {
        const res = await api("/api/query", "POST", body);
        resultEl.textContent = JSON.stringify(res, null, 2);
        resultEl.style.color = "var(--green)";
    } catch (e) {
        resultEl.textContent = `❌ ${e.message}`;
        resultEl.style.color = "var(--red)";
    }
}

// ── ACID Tests ───────────────────────────────────────────────
async function runAcidTest(name) {
    try {
        const results = await api(`/api/acid-test/${name}`, "POST");
        for (const r of results) {
            const el = document.getElementById(`acid-${r.test}`);
            if (el) {
                el.textContent = r.passed ? `✅ PASS — ${r.reason}` : `❌ FAIL — ${r.reason}`;
                el.className = `acid-status ${r.passed ? "pass" : "fail"}`;
            }
        }
        const allPassed = results.every(r => r.passed);
        toast(allPassed ? "All tests passed!" : "Some tests failed", allPassed ? "success" : "error");
    } catch (e) {
        toast(e.message, "error");
    }
}

// ── Metadata Viewer ──────────────────────────────────────────
async function showMetadata() {
    const meta = await api("/api/metadata");
    document.getElementById("meta-content").textContent = JSON.stringify(meta, null, 2);
    document.getElementById("meta-overlay").classList.remove("hidden");
}

// ── Example Queries ──────────────────────────────────────────
const QUERY_EXAMPLES = {
    simple_read: {
        operation: "read",
        entity: "users",
        fields: ["username", "email", "role"]
    },
    gt_filter: {
        operation: "read",
        entity: "readings",
        fields: ["record_id", "temperature", "humidity", "timestamp"],
        conditions: [
            { field: "temperature", op: "gt", value: 25 }
        ],
        order_by: "temperature",
        order: "desc"
    },
    in_filter: {
        operation: "read",
        entity: "users",
        fields: ["username", "email", "role"],
        conditions: [
            { field: "role", op: "in", value: ["admin", "staff"] }
        ]
    },
    like_filter: {
        operation: "read",
        entity: "users",
        fields: ["username", "email", "role"],
        conditions: [
            { field: "username", op: "like", value: "%a%" }
        ]
    },
    or_cond: {
        operation: "read",
        entity: "sensors",
        conditions: [
            {
                or: [
                    { field: "sensor_type", op: "eq", value: "temperature" },
                    { field: "sensor_type", op: "eq", value: "humidity" }
                ]
            }
        ]
    },
    combined: {
        operation: "read",
        entity: "readings",
        fields: ["record_id", "sensor_id", "temperature", "humidity", "timestamp"],
        conditions: [
            { field: "temperature", op: "gte", value: 22 },
            { field: "humidity", op: "lt", value: 65 }
        ],
        order_by: "humidity",
        order: "asc",
        limit: 5
    },
    mongo_filter: {
        operation: "read",
        entity: "sensor_events",
        conditions: [
            { field: "severity", op: "in", value: ["high", "critical"] }
        ]
    },
    create_rec: {
        operation: "create",
        entity: "logs",
        data: {
            log_level: "INFO",
            source: "dashboard",
            message: "Manual test entry from query editor",
            context: { user: "admin", action: "test" }
        }
    }
};

function loadExample(name) {
    const ex = QUERY_EXAMPLES[name];
    if (ex) {
        document.getElementById("query-editor").value = JSON.stringify(ex, null, 2);
        document.getElementById("query-result").textContent = "— example loaded, click Execute —";
        document.getElementById("query-result").style.color = "var(--text-dim)";
    }
}


async function api(url, method = "GET", body = null) {
    const opts = { method, headers: { "Content-Type": "application/json" } };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(url, opts);
    const json = await res.json();
    if (!res.ok || json.error) throw new Error(json.error || `HTTP ${res.status}`);
    return json;
}

// ── Utilities ────────────────────────────────────────────────
function capitalise(s) {
    const str = String(s || "");
    return str.charAt(0).toUpperCase() + str.slice(1).replace(/_/g, " ");
}
function escapeHtml(s) {
    const str = String(s ?? "");
    return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}
function toast(msg, type = "info") {
    const container = document.getElementById("toast-container");
    const el = document.createElement("div");
    el.className = `toast ${type}`;
    el.textContent = msg;
    container.appendChild(el);
    setTimeout(() => el.remove(), 4000);
}
