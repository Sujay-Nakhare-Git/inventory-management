// Shared utilities used across billing.js, inventory.js, and other pages

function escapeHtml(s) {
  const div = document.createElement('div');
  div.textContent = s;
  return div.innerHTML;
}

function round2(value) {
  return Math.round((value + Number.EPSILON) * 100) / 100;
}
