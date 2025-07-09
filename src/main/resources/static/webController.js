
const modeExplanations = {
    all: "Scans all records based on the applied filters.",
    last: "Retrieves the latest record that matches the given filters.",
    lastN: "Scans the last X records from the end offset, where X is user-defined.",
    date: "Retrieves the first record matching the specified date.",
    manual: "Scans all records between the specified start and end offsets.",
    copy: "Copies filtered records to another Kafka Address."
};

let searchCount = 0;
let activeTabId = null;
const abortControllers = {};
let filterMode = "string";
let filterIndex = 0;

function onFilterModeChange() {
    filterMode = document.querySelector('input[name="modeType"]:checked').value;

    const container = document.getElementById("dynamicFilters");
    container.innerHTML = "";
    filterIndex = 0;

    addFilter();

    const addButton = document.querySelector('button[onclick="addFilter()"]');
    if (filterMode === "pattern") {
        addButton.style.display = "none";
    } else {
        addButton.style.display = "inline-block";
    }

    document.getElementById("filtersContainer").style.display = "block";
    document.getElementById("finalSearch").style.display = "block";
}

function loadTopics() {
    const kafkaAddress = document.getElementById("kafkaAddress").value.trim();
    if (!kafkaAddress) return alert("Kafka address is required");

    fetch(`/search/topics?kafkaAddress=${encodeURIComponent(kafkaAddress)}`)
        .then(res => res.json())
        .then(data => {
            allTopics = data.sort((a, b) => a.localeCompare(b));

            document.getElementById("topicSection").style.display = "block";
        })
        .catch(() => alert("Failed to load topics."));
}

function topicSelected() {
    const topic = document.getElementById("topicSelect").value.trim();
    if (!topic) return;

    document.getElementById("searchOptions").style.display = "block";
    document.getElementById("filterModeContainer").style.display = "block";
    document.getElementById("rightInfoMessage").style.display = "none";
    const mode = document.getElementById("searchMode").value;
    updateModExplanation(mode);
    onFilterModeChange();
}

function showFilterStep() {
    const mode = document.getElementById("searchMode").value;

    if (mode === "copy") {
        const targetTopic = document.getElementById("targetTopic").value.trim();
        const targetKafka = document.getElementById("targetKafka").value.trim();

        if (!targetKafka || !targetTopic) {
            alert("Please enter both target Kafka address and target topic before continuing.");
            return;
        }
    }


    document.getElementById("filterModeContainer").style.display = "block";
    onFilterModeChange();
}


window.onload = function () {

    document.getElementById("threadCount").value = userSettings.threads;
    document.getElementById("pollRecords").value = userSettings.pollRecords;
    document.getElementById("timeoutInput").value = userSettings.timeoutMs;

    document.getElementById("searchMode").addEventListener("change", function () {
        document.getElementById("rightInfoMessage").style.display = "none";
        const mode = this.value;
        updateModExplanation(mode);



        document.getElementById("dateContainer").style.display = "none";
        document.getElementById("lastNContainer").style.display = "none";
        document.getElementById("manualContainer").style.display = "none";
        document.getElementById("copyContainer").style.display = "none";


        if (mode === "date") {
            document.getElementById("dateContainer").style.display = "block";
            loadOffsets("unused", "unused", null, null, "datePartitions");
        }

        if (mode === "lastN") {
            document.getElementById("lastNContainer").style.display = "block";
        }

        if (mode === "manual") {
            document.getElementById("manualContainer").style.display = "block";
            loadOffsets("minOffset", "maxOffset");
        }

        if (mode === "copy") {
            document.getElementById("copyContainer").style.display = "block";
            loadOffsets("copyMinOffset", "copyMaxOffset", "copyStartOffset", "copyEndOffset");
        }

        const searchButton = document.getElementById("searchButton");
        if (mode === "copy") {
            searchButton.textContent = "Copy";
        } else {
            searchButton.textContent = "Search";
        }

    });
};

function loadOffsets(minId, maxId, startInputId = null, endInputId = null, partitionElementId = null) {
    const topic = document.getElementById("topicSelect").value;
    const kafkaAddress = document.getElementById("kafkaAddress").value.trim();
    if (!topic || !kafkaAddress) return;

    fetch(`/search/topic-offsets?topic=${topic}&kafkaAddress=${encodeURIComponent(kafkaAddress)}`)
        .then(res => res.json())
        .then(data => {
            if (minId !== "unused") document.getElementById(minId).textContent = data.startOffset;
            if (maxId !== "unused") document.getElementById(maxId).textContent = data.endOffset;

            if (startInputId && endInputId) {
                document.getElementById(startInputId).value = data.startOffset;
                document.getElementById(endInputId).value = data.endOffset;
            }

            if (partitionElementId && data.partitions) {
                document.getElementById(partitionElementId).textContent = data.partitions.join(", ");
            } else if (document.getElementById("partitionList") && data.partitions) {
                document.getElementById("partitionList").textContent = data.partitions.join(", ");
            }
        })
        .catch(() => {
            if (minId !== "unused") document.getElementById(minId).textContent = "?";
            if (maxId !== "unused") document.getElementById(maxId).textContent = "?";

            if (partitionElementId) {
                document.getElementById(partitionElementId).textContent = "N/A";
            } else if (document.getElementById("partitionList")) {
                document.getElementById("partitionList").textContent = "N/A";
            }
        });
}

function generateFilters() {
    const count = parseInt(document.getElementById("filterCount").value);
    const container = document.getElementById("filtersContainer");
    container.innerHTML = "";
    if (isNaN(count) || count < 0) return alert("Invalid filter count");

    if (filterMode === "json") {
        for (let i = 0; i < count; i++) {
            const row = document.createElement("div");
            row.className = "row g-2 mb-2";
            row.innerHTML = `
        <div class="col-md-6"><input type="text" id="key_${i}" class="form-control" placeholder="Key ${i + 1}"></div>
        <div class="col-md-6"><input type="text" id="value_${i}" class="form-control" placeholder="Value ${i + 1}"></div>
      `;
            container.appendChild(row);
        }
    } else {
        for (let i = 0; i < count; i++) {
            const input = document.createElement("input");
            input.type = "text";
            input.id = `raw_${i}`;
            input.className = "form-control mb-2";
            input.placeholder = filterMode === "pattern" ? `Enter regex pattern ${i + 1}` : `Enter filter ${i + 1}`;
            container.appendChild(input);
        }
    }

    document.getElementById("filtersContainer").style.display = "block";
    document.getElementById("finalSearch").style.display = "block";
}

function searchKafka(filterMode, filterIndex) {

    const topic = document.getElementById("topicSelect").value;
    const kafkaAddress = document.getElementById("kafkaAddress").value.trim();
    const mode = document.getElementById("searchMode").value;
    const lastN = mode === "lastN" ? parseInt(document.getElementById("lastNCount").value || 0) : null;
    const startOffset = mode === "manual" ? parseInt(document.getElementById("startOffset").value || 0) : null;
    const endOffset = mode === "manual" ? parseInt(document.getElementById("endOffset").value || 0) : null;
    const id = `result_${++searchCount}`;

    if (mode === "manual") {
        if (isNaN(startOffset) || isNaN(endOffset) || startOffset < 0 || endOffset < startOffset) {
            return alert("Please enter a valid offset range.");
        }
    }

    let body = { topic, mode, lastN, kafkaAddress, requestId: id };
    body.threads = userSettings.threads;
    body.pollRecords = userSettings.pollRecords;
    body.timeoutMs = userSettings.timeoutMs;


    if (mode === "manual") {
        body.startOffset = startOffset;
        body.endOffset = endOffset;
        body.filterMode = filterMode;
    }

    if (mode === "date") {
        const dateKey = document.getElementById("dateKey").value.trim();
        const targetDate = document.getElementById("targetDate").value.trim();
        const partition = parseInt(document.getElementById("partitionInputFirst").value);

        if (!dateKey || !targetDate || isNaN(partition)) {
            return alert("Please provide date key, date (YYYYMMDD), and partition.");
        }

        body.dateKey = dateKey;
        body.date = targetDate;
        body.partition = partition;
        body.filterMode = filterMode;
    }


    if (mode === "copy") {
        const copyStart = parseInt(document.getElementById("copyStartOffset").value || 0);
        const copyEnd = parseInt(document.getElementById("copyEndOffset").value || 0);
        const targetKafka = document.getElementById("targetKafka").value.trim();
        const targetTopic = document.getElementById("targetTopic").value.trim();
        const partition = parseInt(document.getElementById("partitionInput").value || "0");

        if (!targetKafka) return alert("Please enter a target Kafka address.");
        if (!targetTopic) return alert("Please enter a target topic name.");
        if (isNaN(copyStart) || isNaN(copyEnd) || copyStart < 0 || copyEnd < copyStart) {
            return alert("Please enter valid offset range.");
        }

        body.startOffset = copyStart;
        body.endOffset = copyEnd;
        body.partition = partition;
        body.targetKafka = targetKafka;
        body.targetTopic = targetTopic;
        body.filterMode = filterMode;
    }


    if (filterMode === "json") {
        let filters = {};
        for (let i = 0; i < filterIndex; i++) {
            const keyInput = document.getElementById(`key_${i}`);
            const valueInput = document.getElementById(`value_${i}`);
            if (keyInput && valueInput) {
                const key = keyInput.value.trim();
                const value = valueInput.value.trim();
                if (!key || !value) return alert(`Eksik filtre`);
                filters[key] = value;
            }
        }
        body.filters = filters;
    } else {
        const rawFilters = [];
        for (let i = 0; i < filterIndex; i++) {
            const input = document.getElementById(`raw_${i}`);
            if (input) {
                const value = input.value.trim();
                if (!value) return alert(`Eksik string filtre`);
                rawFilters.push(value);
            }
        }
        body.rawFilters = rawFilters;
    }


    const controller = new AbortController();
    abortControllers[id] = controller;


    const tab = document.createElement("div");
    tab.className = "result-tab";
    tab.id = id + "_tab";
    tab.innerHTML = `
      Result ${searchCount}
      <span class="spinner-border spinner-border-sm" role="status"></span>
      <span class="close-x" onclick="event.stopPropagation(); killResult('${id}')">&times;</span>
    `;
    tab.onclick = () => toggleResult(id);
    document.getElementById("resultTabs").appendChild(tab);

    const box = document.createElement("div");
    box.className = "result-box";
    box.id = id + "_box";
    box.style.display = "none";
    document.getElementById("resultBoxes").appendChild(box);

    let url;
    if (filterMode === "json") {
        url = "/search";
    } else if (filterMode === "pattern") {
        url = "/search/pattern-search";
    } else {
        url = "/search/simple-string-search";
    }

    fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal
    })
        .then(res => res.json())
        .then(data => {

            const spinner = tab.querySelector(".spinner-border");
            if (spinner) spinner.remove();

            box.innerHTML = "";

            const summary = document.createElement("div");
            summary.className = "filter-summary";
            summary.innerHTML = `
<div><strong>Filter Type:</strong> ${filterMode === "json"
                    ? "JSON"
                    : filterMode === "pattern"
                        ? "Pattern"
                        : "String"
                }</div>
  <div><strong>Filters:</strong> ${filterMode === "json"
                    ? Object.entries(body.filters).map(([k, v]) => `${k}=${v}`).join(", ")
                    : body.rawFilters.join(", ")
                }</div>
  <div><strong>Mode:</strong> ${mode}${mode === "lastN" ? ` (${lastN})` : ""}${mode === "manual" ? ` (Offsets: ${startOffset} - ${endOffset})` : ""}</div>
  <div><strong>Results Found:</strong> ${data.length}</div>
  <hr style="margin: 0.4rem 0;" />
`;


            summary.style.position = "relative";


            const reverseBtn = document.createElement("button");
            reverseBtn.className = "btn btn-sm btn-outline-secondary me-2";
            reverseBtn.style.position = "absolute";
            reverseBtn.style.bottom = "2rem";
            reverseBtn.style.right = "0.4rem";
            reverseBtn.textContent = "↕ Reverse";
            reverseBtn.title = "Reverse result order";
            reverseBtn.onclick = () => {
                const entries = Array.from(box.querySelectorAll(".record-entry"));
                entries.reverse().forEach(entry => box.appendChild(entry));
            };
            summary.appendChild(reverseBtn);


            const downloadBtn = document.createElement("button");
            downloadBtn.className = "btn btn-sm btn-outline-primary";
            downloadBtn.style.position = "absolute";
            downloadBtn.style.top = "-5rem";
            downloadBtn.style.right = "0.5rem";
            downloadBtn.title = "Download results";
            downloadBtn.innerHTML = "⬇ Download";

            downloadBtn.onclick = () => {
                let headerLines = [];
                headerLines.push(`Filter Type: ${filterMode === "json" ? "JSON" : "String"}`);

                if (filterMode === "json") {
                    headerLines.push("Filters: " + Object.entries(body.filters).map(([k, v]) => `${k}=${v}`).join(", "));
                } else {
                    headerLines.push("Filters: " + body.rawFilters.join(", "));
                }

                headerLines.push(`Mode: ${mode}${mode === "lastN" ? ` (${lastN})` : ""}${mode === "manual" ? ` (Offsets: ${startOffset} - ${endOffset})` : ""}`);

                const resultsText = Array.from(box.querySelectorAll(".record-entry"))
                    .map(div => div.textContent)
                    .join("\n\n");

                const textContent = headerLines.join("\n") + "\n\n" + resultsText;

                const blob = new Blob([textContent], { type: "text/plain" });
                const url = URL.createObjectURL(blob);
                const a = document.createElement("a");
                a.href = url;
                a.download = id + ".txt";
                a.click();
                URL.revokeObjectURL(url);
            };

            summary.appendChild(downloadBtn);
            box.appendChild(summary);

            if (!data || data.length === 0) {
                box.innerHTML += "<em>No matching results.</em>";
                return;
            }

            data.forEach(record => {
                const div = document.createElement("div");
                div.className = "record-entry";
                try {
                    const parsed = JSON.parse(record);
                    div.innerHTML = `<strong>Offset:</strong> ${parsed.offset}<br><pre>${JSON.stringify(parsed.value, null, 2)}</pre>`;
                } catch {
                    div.textContent = record;
                }
                box.appendChild(div);
            });
        })
        .catch(err => {
            if (err.name === "AbortError") return;
            alert("An error occurred during the Kafka query.");
        });
}

function toggleResult(id) {
    const allTabs = document.querySelectorAll(".result-tab");
    const allBoxes = document.querySelectorAll(".result-box");
    allTabs.forEach(tab => tab.classList.remove("active"));
    allBoxes.forEach(box => (box.style.display = "none"));

    const tab = document.getElementById(id + "_tab");
    const box = document.getElementById(id + "_box");
    if (activeTabId === id) {
        activeTabId = null;
    } else {
        tab.classList.add("active");
        box.style.display = "block";
        activeTabId = id;
    }
}

function killResult(id) {
    const tab = document.getElementById(id + "_tab");
    const box = document.getElementById(id + "_box");
    if (tab) tab.remove();
    if (box) box.remove();
    if (activeTabId === id) activeTabId = null;

    if (abortControllers[id]) {
        abortControllers[id].abort();
        delete abortControllers[id];
    }

    fetch("/search/cancel", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ requestId: id })
    });
}



function addFilter(key = "", value = "") {
    filterMode = document.querySelector('input[name="modeType"]:checked').value;

    const container = document.getElementById("dynamicFilters");


    if (filterMode === "pattern") {
        const existing = document.querySelectorAll(`#dynamicFilters input[id^="raw_"]`);
        if (existing.length >= 1) return;
    }

    const row = document.createElement("div");
    row.className = "row g-2 mb-2 align-items-center";
    row.id = `filterRow_${filterIndex}`;

    if (filterMode === "json") {
        row.innerHTML = `
      <div class="col-md-5">
        <input type="text" class="form-control" id="key_${filterIndex}" placeholder="Key" value="${key}">
      </div>
      <div class="col-md-5">
        <input type="text" class="form-control" id="value_${filterIndex}" placeholder="Value" value="${value}">
      </div>
      <div class="col-md-2 text-end">
        <button class="btn btn-sm btn-outline-danger" onclick="removeFilter(${filterIndex})">−</button>
      </div>
    `;
    } else {
        row.innerHTML = `
      <div class="col-md-10">
        <input type="text" class="form-control" id="raw_${filterIndex}" placeholder="${filterMode === 'pattern' ? 'Regex pattern' : 'String filter'}" value="${key}">
      </div>
      <div class="col-md-2 text-end">
        <button class="btn btn-sm btn-outline-danger" onclick="removeFilter(${filterIndex})">−</button>
      </div>
    `;
    }

    container.appendChild(row);
    filterIndex++;


    if (filterMode === "pattern") {
        document.querySelector('button[onclick="addFilter()"]').style.display = "none";
    }
}


function removeFilter(index) {
    const row = document.getElementById(`filterRow_${index}`);
    if (row) {
        row.remove();


        if (filterMode === "pattern") {
            document.querySelector('button[onclick="addFilter()"]').style.display = "inline-block";
        }

        filterIndex--;
    }
}
function updateModExplanation(mode) {
    const explanationBox = document.getElementById("modExplanation");
    const explanationText = document.getElementById("modExplanationText");

    if (modeExplanations[mode]) {
        explanationBox.style.display = "block";
        explanationText.textContent = modeExplanations[mode];
    } else {
        explanationBox.style.display = "none";
    }
}

let allTopics = [];

function filterTopics() {
    const input = document.getElementById("topicSelect").value.trim().toLowerCase();
    const box = document.getElementById("autocompleteBox");
    box.innerHTML = "";

    if (!input) {
        allTopics.forEach(topic => {
            const div = document.createElement("div");
            div.textContent = topic;
            div.onclick = () => {
                document.getElementById("topicSelect").value = topic;
                box.innerHTML = "";
                topicSelected();
            };
            box.appendChild(div);
        });
        return;
    }

    const matches = allTopics.filter(topic => topic.toLowerCase().includes(input));
    matches.forEach(topic => {
        const div = document.createElement("div");
        div.textContent = topic;
        div.onclick = () => {
            document.getElementById("topicSelect").value = topic;
            box.innerHTML = "";
            topicSelected();
        };
        box.appendChild(div);
    });
}

document.addEventListener("click", function (e) {
    if (!e.target.closest("#topicSelect") && !e.target.closest("#autocompleteBox")) {
        document.getElementById("autocompleteBox").innerHTML = "";
    }
});

function showAllTopics() {
    const box = document.getElementById("autocompleteBox");
    box.innerHTML = "";

    allTopics.forEach(topic => {
        const div = document.createElement("div");
        div.textContent = topic;
        div.onclick = () => {
            document.getElementById("topicSelect").value = topic;
            box.innerHTML = "";
            topicSelected();
        };
        box.appendChild(div);
    });
}

let userSettings = {
    threads: 4,
    pollRecords: 500,
    timeoutMs: 30000
};

function toggleSettingsPanel() {
    const panel = document.getElementById("settingsPanel");
    panel.style.right = panel.style.right === "0px" ? "-320px" : "0px";
}

function saveSettings() {
    const threads = parseInt(document.getElementById("threadCount").value);
    const records = parseInt(document.getElementById("pollRecords").value);
    const timeout = parseInt(document.getElementById("timeoutInput").value);

    if (isNaN(threads) || threads <= 0) {
        alert("Please enter a valid thread count (must be > 0)");
        return;
    }

    if (isNaN(records) || records <= 0 || records > 5000) {
        alert("Please enter a valid poll record count (1 to 5000)");
        return;
    }

    if (isNaN(timeout) || timeout < 1000 || timeout % 1000 !== 0) {
        alert("Please enter a timeout of at least 1000 ms, in multiples of 1000.");
        return;
    }

    userSettings.threads = threads;
    userSettings.pollRecords = records;
    userSettings.timeoutMs = timeout;

    alert(`Settings saved:\nThreads: ${threads}\nPoll Records: ${records}\nTimeout: ${timeout} ms`);
    toggleSettingsPanel();
}