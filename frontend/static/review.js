// Review page JavaScript

let terms = [];
let selectedTermIds = new Set();
let currentTerm = null;

// DOM elements
const termsList = document.getElementById('termsList');
const statusFilter = document.getElementById('statusFilter');
const confidenceFilter = document.getElementById('confidenceFilter');
const refreshBtn = document.getElementById('refreshBtn');
const bulkApproveBtn = document.getElementById('bulkApproveBtn');
const publishBtn = document.getElementById('publishBtn');
const termModal = document.getElementById('termModal');
const closeModal = document.getElementById('closeModal');

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadStats();
    loadTerms();

    // Event listeners
    statusFilter.addEventListener('change', loadTerms);
    confidenceFilter.addEventListener('change', loadTerms);
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

    let url = '/api/v1/terms?limit=100';
    if (status) url += `&status=${status}`;
    if (confidence) url += `&confidence=${confidence}`;

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
        <div class="term-card" data-id="${term.id}">
            <input type="checkbox" class="term-checkbox"
                   ${selectedTermIds.has(term.id) ? 'checked' : ''}
                   onclick="toggleSelection(event, '${term.id}')">
            <div class="term-content" onclick="openTermModal('${term.id}')">
                <div class="term-header">
                    <span class="term-name">${escapeHtml(term.name)}</span>
                    <span class="badge badge-${term.confidence}">${term.confidence}</span>
                    <span class="badge badge-${term.status}">${formatStatus(term.status)}</span>
                </div>
                <p class="term-definition">${escapeHtml(term.definition)}</p>
                <div class="term-meta">
                    <span class="tag">Queries: ${term.query_frequency || 0}</span>
                    <span class="tag">Users: ${term.user_access_count || 0}</span>
                </div>
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
    document.getElementById('modalDefinition').value = currentTerm.edited_definition || currentTerm.definition;
    document.getElementById('modalShortDesc').value = currentTerm.short_description || '';
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

    // Sources
    const sourcesList = document.getElementById('modalSources');
    sourcesList.innerHTML = (currentTerm.source_assets || [])
        .map(src => `<li>${escapeHtml(src)}</li>`)
        .join('') || '<li>No sources</li>';

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

function formatStatus(status) {
    return status.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}
