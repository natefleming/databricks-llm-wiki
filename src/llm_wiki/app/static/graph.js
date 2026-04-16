/* LLM Wiki - Knowledge graph visualization using Cytoscape.js */

let cy = null;

const TYPE_COLORS = {
    concept: '#4da6ff',
    entity: '#81c784',
    source: '#ffb74d',
    analysis: '#ce93d8',
    index: '#90a4ae',
};

async function initGraph(centerPageId) {
    const url = centerPageId
        ? `/api/graph?center=${encodeURIComponent(centerPageId)}`
        : '/api/graph';

    const container = document.getElementById('cy');

    try {
        container.innerHTML = '<p style="padding:2rem;color:#666;">Loading graph...</p>';

        const resp = await fetch(url, { credentials: 'include' });
        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
        }
        const data = await resp.json();

        if (!data.nodes || data.nodes.length === 0) {
            container.innerHTML = '<p style="padding:2rem;color:#666;">No pages to display in graph.</p>';
            return;
        }

        container.innerHTML = '';

        cy = cytoscape({
            container: container,
            elements: [...data.nodes, ...data.edges],

            style: [
                {
                    selector: 'node',
                    style: {
                        'label': 'data(label)',
                        'background-color': function (ele) {
                            return TYPE_COLORS[ele.data('type')] || '#90a4ae';
                        },
                        'color': '#333',
                        'font-size': '11px',
                        'text-valign': 'bottom',
                        'text-margin-y': 5,
                        'width': 30,
                        'height': 30,
                    },
                },
                {
                    selector: 'edge',
                    style: {
                        'width': 1,
                        'line-color': '#ccc',
                        'curve-style': 'bezier',
                        'target-arrow-shape': 'triangle',
                        'target-arrow-color': '#ccc',
                        'arrow-scale': 0.8,
                    },
                },
                {
                    selector: ':selected',
                    style: {
                        'background-color': '#ff6b6b',
                        'border-width': 2,
                        'border-color': '#ff6b6b',
                    },
                },
            ],

            layout: {
                name: 'cose',
                animate: true,
                animationDuration: 500,
                nodeOverlap: 20,
                idealEdgeLength: 100,
                nodeRepulsion: 400000,
            },
        });

        /* Click to navigate */
        cy.on('tap', 'node', function (evt) {
            const pageId = evt.target.data('id');
            window.location.href = `/page/${pageId}`;
        });

        /* Hover highlight */
        cy.on('mouseover', 'node', function (evt) {
            const node = evt.target;
            const neighborhood = node.neighborhood().add(node);
            cy.elements().not(neighborhood).style('opacity', 0.2);
        });

        cy.on('mouseout', 'node', function () {
            cy.elements().style('opacity', 1);
        });

    } catch (e) {
        console.error('Graph init failed:', e);
        container.innerHTML =
            `<p style="padding:2rem;color:#666;">Could not load graph data: ${e.message}</p>`;
    }
}

function resetLayout() {
    if (cy) {
        cy.layout({ name: 'cose', animate: true }).run();
    }
}

function fitGraph() {
    if (cy) {
        cy.fit(undefined, 30);
    }
}
