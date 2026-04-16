/* LLM Wiki - Page navigation and theme toggle */

function toggleTheme() {
    const html = document.documentElement;
    const current = html.getAttribute('data-theme');
    const next = current === 'dark' ? 'light' : 'dark';
    html.setAttribute('data-theme', next);
    localStorage.setItem('wiki-theme', next);

    const icon = document.getElementById('theme-icon');
    if (icon) {
        icon.textContent = next === 'dark' ? '\u2600' : '\u263D';
    }
}

/* Restore saved theme */
(function () {
    const saved = localStorage.getItem('wiki-theme');
    if (saved) {
        document.documentElement.setAttribute('data-theme', saved);
        const icon = document.getElementById('theme-icon');
        if (icon) {
            icon.textContent = saved === 'dark' ? '\u2600' : '\u263D';
        }
    }
})();

/* Keyboard shortcut: / to focus search */
document.addEventListener('keydown', function (e) {
    if (e.key === '/' && !e.ctrlKey && !e.metaKey) {
        const input = document.querySelector('.search-input');
        if (input && document.activeElement !== input) {
            e.preventDefault();
            input.focus();
        }
    }
});
