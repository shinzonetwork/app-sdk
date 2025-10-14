// Connect to Server-Sent Events
let eventSource = null;
let reconnectTimeout = null;

// Track previous log counts and displayed logs
let previousCounts = {
    unfiltered: 0,
    filtered: 0
};
let displayedLogs = {
    unfiltered: [],
    filtered: []
};

function connectSSE() {
    // Close existing connection if any
    if (eventSource) {
        eventSource.close();
    }

    eventSource = new EventSource('/api/stream');

    eventSource.onopen = function() {
        console.log('✓ Connected to server');
        updateConnectionStatus(true);
    };

    eventSource.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            updateDashboard(data);
        } catch (error) {
            console.error('Error parsing data:', error);
        }
    };

    eventSource.onerror = function(error) {
        console.error('SSE Error:', error);
        updateConnectionStatus(false);
        
        // Attempt to reconnect after 5 seconds
        if (reconnectTimeout) {
            clearTimeout(reconnectTimeout);
        }
        reconnectTimeout = setTimeout(() => {
            console.log('Attempting to reconnect...');
            connectSSE();
        }, 5000);
    };
}

function updateConnectionStatus(connected) {
    const indicator = document.getElementById('connectionStatus');
    const text = document.getElementById('connectionText');
    
    if (connected) {
        indicator.className = 'status-indicator connected';
        text.textContent = 'Connected';
    } else {
        indicator.className = 'status-indicator connecting';
        text.textContent = 'Reconnecting...';
    }
}

function updateDashboard(data) {
    // Update system status
    document.getElementById('uptime').textContent = data.uptime;
    document.getElementById('lastUpdate').textContent = data.lastUpdateAgo;

    // Update view names
    document.getElementById('unfilteredViewName').textContent = data.unfilteredViewName;
    document.getElementById('filteredViewName').textContent = data.filteredViewName;

    // Update unfiltered logs - only refresh if count changed
    const unfilteredCountChanged = data.unfilteredLogs.length !== previousCounts.unfiltered;
    if (unfilteredCountChanged) {
        previousCounts.unfiltered = data.unfilteredLogs.length;
        displayedLogs.unfiltered = selectRandomLogs(data.unfilteredLogs, 5);
        updateLogs('unfilteredLogs', data.unfilteredLogs, displayedLogs.unfiltered);
    }

    // Update filtered logs - only refresh if count changed
    const filteredCountChanged = data.filteredLogs.length !== previousCounts.filtered;
    if (filteredCountChanged) {
        previousCounts.filtered = data.filteredLogs.length;
        displayedLogs.filtered = selectRandomLogs(data.filteredLogs, 5);
        updateLogs('filteredLogs', data.filteredLogs, displayedLogs.filtered);
    }

    // Update statistics
    document.getElementById('unfilteredCount').textContent = data.stats.unfilteredCount;
    document.getElementById('filteredCount').textContent = data.stats.filteredCount;
}

function selectRandomLogs(logs, count) {
    if (!logs || logs.length === 0) {
        return [];
    }
    
    if (logs.length <= count) {
        return logs;
    }
    
    // Get random indices
    const indices = new Set();
    while (indices.size < count) {
        indices.add(Math.floor(Math.random() * logs.length));
    }
    return Array.from(indices).map(i => logs[i]);
}

function updateLogs(containerId, allLogs, displayLogs) {
    const container = document.getElementById(containerId);
    
    if (!allLogs || allLogs.length === 0) {
        container.innerHTML = '<div class="loading">No logs received yet... waiting for data...</div>';
        return;
    }

    let html = '';
    displayLogs.forEach((log) => {
        html += `
            <div class="log-item">
                <span class="log-hash">${log.transactionHash}</span>
            </div>
        `;
    });
    
    // Add "..." if there are more logs
    if (allLogs.length > displayLogs.length) {
        html += '<div class="log-more">...</div>';
    }

    container.innerHTML = html;
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Shinzo Web Demo App initialized');
    connectSSE();
    
    // Also fetch initial data
    fetch('/api/data')
        .then(response => response.json())
        .then(data => updateDashboard(data))
        .catch(error => console.error('Error fetching initial data:', error));
});

// Cleanup on page unload
window.addEventListener('beforeunload', function() {
    if (eventSource) {
        eventSource.close();
    }
    if (reconnectTimeout) {
        clearTimeout(reconnectTimeout);
    }
});

