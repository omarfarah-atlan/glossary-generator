// Review page JavaScript

let terms = [];
let selectedTermIds = new Set();
let currentTerm = null;
let atlanBaseUrl = '';

// DOM elements
const termsList = document.getElementById('termsList');
const statusFilter = document.getElementById('statusFilter');
const confidenceFilter = document.getElementById('confidenceFilter');
const termTypeFilter = document.getElementById('termTypeFilter');
const refreshBtn = document.getElementById('refreshBtn');
const bulkApproveBtn = document.getElementById('bulkApproveBtn');
const publishBtn = document.getElementById('publishBtn');
const termModal = document.getElementById('termModal');
const closeModal = document.getElementById('closeModal');

// Initialize
document.addEventListener('DOMContentLoaded', async () => {
    // Load Atlan base URL for asset links
    try {
        const settingsResp = await fetch('/api/v1/settings');
        const settings = await settingsResp.json();
        atlanBaseUrl = (settings.atlan_base_url || '').replace(/\/+$/, '');
    } catch (e) { /* ignore */ }

    loadStats();
    loadTerms();

    // Event listeners
    statusFilter.addEventListener('change', loadTerms);
    confidenceFilter.addEventListener('change', loadTerms);
    termTypeFilter.addEventListener('change', loadTerms);
    refreshBtn.addEventListener('click', () => {
        loadStats();
        loadTerms();
    });
    bulkApproveBtn.addEventListener('click', bulkApprove);
    publishBtn.addEventListener('click', publishApproved);
    closeModal.addEventListener('click', () => termModal.classList.remove('active'));
    document.getElementById('modalApproveBtn').addEventListener('click', approveTerm);
    document.getElementById('modalRejectBtn').addEventListener('click', rejectTerm);

    // Close modal on background click
    termModal.addEventListener('click', (e) => {
        if (e.target === termModal) {
            termModal.classList.remove('active');
        }
    });
});

async function loadStats() {
    try {
        const response = await fetch('/api/v1/stats');
        const stats = await response.json();

        document.getElementById('statTotal').textContent = stats.total || 0;
        document.getElementById('statPending').textContent = stats.by_status?.pending_review || 0;
        document.getElementById('statApproved').textContent = stats.by_status?.approved || 0;
        document.getElementById('statRejected').textContent = stats.by_status?.rejected || 0;
        document.getElementById('statPublished').textContent = stats.by_status?.published || 0;

        // Enable/disable publish button based on approved count
        publishBtn.disabled = (stats.by_status?.approved || 0) === 0;
    } catch (error) {
        console.error('Error loading stats:', error);
    }
}

async function loadTerms() {
    termsList.innerHTML = '<p class="loading">Loading terms...</p>';

    const status = statusFilter.value;
    const confidence = confidenceFilter.value;
    const termType = termTypeFilter.value;

    let url = '/api/v1/terms?limit=100';
    if (status) url += `&status=${status}`;
    if (confidence) url += `&confidence=${confidence}`;
    if (termType) url += `&term_type=${termType}`;

    try {
        const response = await fetch(url);
        const data = await response.json();
        terms = data.terms || [];

        renderTerms();
    } catch (error) {
        console.error('Error loading terms:', error);
        termsList.innerHTML = '<p class="loading">Error loading terms</p>';
    }
}

function renderTerms() {
    if (terms.length === 0) {
        termsList.innerHTML = `
            <div class="empty-state">
                <h3>No terms found</h3>
                <p>Try adjusting your filters or generate new terms.</p>
            </div>
        `;
        return;
    }

    termsList.innerHTML = terms.map(term => `
        <div class="term-card${isNewTerm(term) ? ' is-new' : ''}" data-id="${term.id}">
            <input type="checkbox" class="term-checkbox"
                   ${selectedTermIds.has(term.id) ? 'checked' : ''}
                   onclick="toggleSelection(event, '${term.id}')">
            <div class="term-content" onclick="openTermModal('${term.id}')">
                <div class="term-header">
                    <span class="term-name">${escapeHtml(term.name)}</span>
                    ${isNewTerm(term) ? '<span class="badge badge-new">NEW</span>' : ''}
                    <span class="badge badge-${term.term_type || 'business_term'}">${formatTermType(term.term_type)}</span>
                    <span class="badge badge-${term.confidence}">${term.confidence}</span>
                    <span class="badge badge-${term.status}">${formatStatus(term.status)}</span>
                </div>
                <p class="term-definition">${escapeHtml(term.definition)}</p>
            </div>
        </div>
    `).join('');

    updateBulkActions();
}

function toggleSelection(event, termId) {
    event.stopPropagation();
    if (selectedTermIds.has(termId)) {
        selectedTermIds.delete(termId);
    } else {
        selectedTermIds.add(termId);
    }
    updateBulkActions();
}

function updateBulkActions() {
    bulkApproveBtn.disabled = selectedTermIds.size === 0;
}

function openTermModal(termId) {
    currentTerm = terms.find(t => t.id === termId);
    if (!currentTerm) return;

    document.getElementById('modalTermName').textContent = currentTerm.name;
    document.getElementById('modalConfidence').textContent = currentTerm.confidence;
    document.getElementById('modalConfidence').className = `badge badge-${currentTerm.confidence}`;
    document.getElementById('modalStatus').textContent = formatStatus(currentTerm.status);
    document.getElementById('modalStatus').className = `badge badge-${currentTerm.status}`;
    document.getElementById('modalTermType').textContent = formatTermType(currentTerm.term_type);
    document.getElementById('modalTermType').className = `badge badge-${currentTerm.term_type || 'business_term'}`;
    document.getElementById('modalDefinition').value = currentTerm.edited_definition || currentTerm.definition;
    document.getElementById('modalNotes').value = currentTerm.reviewer_notes || '';

    // Examples
    const examplesList = document.getElementById('modalExamples');
    examplesList.innerHTML = (currentTerm.examples || [])
        .map(ex => `<li>${escapeHtml(ex)}</li>`)
        .join('') || '<li>No examples</li>';

    // Synonyms
    const synonymsDiv = document.getElementById('modalSynonyms');
    synonymsDiv.innerHTML = (currentTerm.synonyms || [])
        .map(syn => `<span class="tag">${escapeHtml(syn)}</span>`)
        .join('') || '<span class="tag">None</span>';

    // Sources — linked to Atlan if base URL is available
    const sourcesDiv = document.getElementById('modalSources');
    const sourceAssets = currentTerm.source_assets || [];
    if (sourceAssets.length === 0) {
        sourcesDiv.innerHTML = '<span class="text-muted">No sources</span>';
    } else {
        sourcesDiv.innerHTML = sourceAssets.map(qn => {
            const shortName = currentTerm.source_asset_name || qn.split('/').pop();
            const assetType = currentTerm.source_asset_type || '';
            const typeLabel = assetType ? `<span class="source-asset-type">${escapeHtml(assetType)}</span>` : '';
            if (atlanBaseUrl) {
                const url = `${atlanBaseUrl}/assets/${encodeURIComponent(qn)}`;
                return `<a href="${url}" target="_blank" rel="noopener" class="source-asset-link">${typeLabel}<span class="source-asset-name">${escapeHtml(shortName)}</span><svg width="12" height="12" viewBox="0 0 12 12" fill="currentColor" class="external-icon"><path d="M9 3L5 7m4-4H6.5M9 3v2.5M4 3H3a1 1 0 00-1 1v5a1 1 0 001 1h5a1 1 0 001-1V8"/></svg></a>`;
            }
            return `<span class="source-asset-link">${typeLabel}<span class="source-asset-name">${escapeHtml(shortName)}</span></span>`;
        }).join('');
    }

    // Generation context ("Why this term?")
    const contextEl = document.getElementById('generationContext');
    const reasoning = currentTerm.generation_reasoning;
    const signals = currentTerm.metadata_signals || [];
    const hasContext = reasoning || signals.length > 0 || currentTerm.source_asset_name;

    if (hasContext) {
        contextEl.style.display = 'block';
        // Auto-expand if there's reasoning to show
        contextEl.classList.add('expanded');

        // Reasoning from LLM
        const reasoningEl = document.getElementById('modalReasoning');
        reasoningEl.innerHTML = reasoning
            ? `<p>${escapeHtml(reasoning)}</p>`
            : '';

        // Source info
        const sourceInfo = document.getElementById('modalSourceInfo');
        const parts = [];
        if (currentTerm.source_asset_name) {
            let label = `<strong>${escapeHtml(currentTerm.source_asset_name)}</strong>`;
            if (currentTerm.source_asset_type) label += ` (${escapeHtml(currentTerm.source_asset_type)})`;
            parts.push(`Source: ${label}`);
        }
        if (currentTerm.source_database || currentTerm.source_schema) {
            const loc = [currentTerm.source_database, currentTerm.source_schema].filter(Boolean).join('.');
            parts.push(`Location: <code>${escapeHtml(loc)}</code>`);
        }
        sourceInfo.innerHTML = parts.join(' &bull; ');

        // Metadata signals
        const signalsEl = document.getElementById('modalSignals');
        signalsEl.innerHTML = signals.length > 0
            ? 'Signals: ' + signals.map(s => `<span class="signal-tag">${escapeHtml(s)}</span>`).join('')
            : '';

        // Usage info
        const usageEl = document.getElementById('modalUsageInfo');
        const usageParts = [];
        if (currentTerm.query_frequency > 0) usageParts.push(`${currentTerm.query_frequency} queries`);
        if (currentTerm.user_access_count > 0) usageParts.push(`${currentTerm.user_access_count} users`);
        if (currentTerm.popularity_score > 0) usageParts.push(`Popularity: ${currentTerm.popularity_score.toFixed(1)}`);
        usageEl.innerHTML = usageParts.length > 0
            ? 'Usage: ' + usageParts.join(' &bull; ')
            : '';
    } else {
        contextEl.style.display = 'none';
    }

    // Update button states based on status
    const approveBtn = document.getElementById('modalApproveBtn');
    const rejectBtn = document.getElementById('modalRejectBtn');

    if (currentTerm.status === 'approved' || currentTerm.status === 'published') {
        approveBtn.disabled = true;
        rejectBtn.disabled = currentTerm.status === 'published';
    } else if (currentTerm.status === 'rejected') {
        approveBtn.disabled = false;
        rejectBtn.disabled = true;
    } else {
        approveBtn.disabled = false;
        rejectBtn.disabled = false;
    }

    termModal.classList.add('active');
}

async function approveTerm() {
    if (!currentTerm) return;

    const editedDefinition = document.getElementById('modalDefinition').value;
    const notes = document.getElementById('modalNotes').value;

    try {
        const response = await fetch(`/api/v1/terms/${currentTerm.id}/approve`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                edited_definition: editedDefinition !== currentTerm.definition ? editedDefinition : null,
                reviewer_notes: notes || null,
            }),
        });

        if (response.ok) {
            termModal.classList.remove('active');
            loadStats();
            loadTerms();
        } else {
            const error = await response.json();
            alert('Error: ' + (error.detail || 'Failed to approve term'));
        }
    } catch (error) {
        console.error('Error approving term:', error);
        alert('Error approving term');
    }
}

async function rejectTerm() {
    if (!currentTerm) return;

    const reason = document.getElementById('modalNotes').value || prompt('Please provide a reason for rejection:');
    if (!reason) return;

    try {
        const response = await fetch(`/api/v1/terms/${currentTerm.id}/reject`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ reason }),
        });

        if (response.ok) {
            termModal.classList.remove('active');
            loadStats();
            loadTerms();
        } else {
            const error = await response.json();
            alert('Error: ' + (error.detail || 'Failed to reject term'));
        }
    } catch (error) {
        console.error('Error rejecting term:', error);
        alert('Error rejecting term');
    }
}

async function bulkApprove() {
    if (selectedTermIds.size === 0) return;

    if (!confirm(`Approve ${selectedTermIds.size} selected terms?`)) return;

    try {
        const response = await fetch('/api/v1/terms/bulk-approve', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ term_ids: Array.from(selectedTermIds) }),
        });

        const result = await response.json();

        if (result.approved > 0) {
            alert(`Approved ${result.approved} terms${result.failed > 0 ? `, ${result.failed} failed` : ''}`);
            selectedTermIds.clear();
            loadStats();
            loadTerms();
        } else {
            alert('Failed to approve terms: ' + (result.errors?.join(', ') || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error in bulk approve:', error);
        alert('Error approving terms');
    }
}

async function publishApproved() {
    // Get all approved term IDs
    const approvedTerms = terms.filter(t => t.status === 'approved');
    if (approvedTerms.length === 0) {
        alert('No approved terms to publish');
        return;
    }

    if (!confirm(`Publish ${approvedTerms.length} approved terms to Atlan?`)) return;

    try {
        const response = await fetch('/api/v1/terms/publish', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ term_ids: approvedTerms.map(t => t.id) }),
        });

        const result = await response.json();

        if (result.published > 0) {
            alert(`Published ${result.published} terms${result.failed > 0 ? `, ${result.failed} failed` : ''}`);
            loadStats();
            loadTerms();
        } else {
            alert('Failed to publish terms: ' + (result.errors?.join(', ') || 'Unknown error'));
        }
    } catch (error) {
        console.error('Error publishing:', error);
        alert('Error publishing terms');
    }
}

async function clearAllTerms() {
    if (!confirm('Delete ALL draft terms? This cannot be undone.')) return;

    try {
        const response = await fetch('/api/v1/terms', { method: 'DELETE' });
        const result = await response.json();
        alert(`Deleted ${result.deleted} terms.`);
        selectedTermIds.clear();
        loadStats();
        loadTerms();
    } catch (error) {
        console.error('Error clearing terms:', error);
        alert('Error clearing terms');
    }
}

function formatStatus(status) {
    return status.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function formatTermType(termType) {
    const labels = {
        'business_term': 'Business Term',
        'metric': 'Metric',
        'dimension': 'Dimension',
    };
    return labels[termType] || 'Business Term';
}

function isNewTerm(term) {
    if (!term.created_at) return false;
    const created = new Date(term.created_at);
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    return created > oneHourAgo;
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
