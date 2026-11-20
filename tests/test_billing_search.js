const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

function createSearch() {
    const items = [
        { dataset: { search: 'Cotton trousers B-001 Bottoms Size 1 Blue' }, style: {} },
        { dataset: { search: 'Linen trousers B-002 Bottoms Size 2 Black' }, style: {} },
        { dataset: { search: 'Silk dress D-003 Dresses Free Size Red' }, style: {} },
    ];
    const elements = Object.fromEntries([
        'productSearch', 'noMatchingProducts', 'customerName', 'customerPhone',
        'rentalDays', 'rentAmount', 'depositAmount',
    ].map(id => [id, {
        value: '', hidden: true, listeners: {},
        addEventListener(event, listener) { this.listeners[event] = listener; },
    }]));
    const document = {
        getElementById(id) {
            assert.ok(elements[id], `Unexpected element: ${id}`);
            return elements[id];
        },
        querySelectorAll(selector) {
            assert.equal(selector, '#productList .product-item');
            return items;
        },
        addEventListener() {},
    };
    const source = fs.readFileSync(path.join(__dirname, '../static/billing.js'), 'utf8');
    vm.runInNewContext(source, { document });
    return query => {
        elements.productSearch.value = query;
        elements.productSearch.listeners.input.call(elements.productSearch);
        return {
            visible: items.map(item => item.style.display !== 'none'),
            noMatches: !elements.noMatchingProducts.hidden,
        };
    };
}

test('filters on each input event, including partial codes and case-insensitive words', () => {
    const search = createSearch();
    assert.deepEqual(search('b-00').visible, [true, true, false]);
    assert.deepEqual(search('b-001').visible, [true, false, false]);
    assert.deepEqual(search('b-00').visible, [true, true, false]);
    assert.deepEqual(search('COT').visible, [true, false, false]);
    assert.deepEqual(search('ress').visible, [false, false, true]);
});

test('matches partial terms across fields regardless of order or extra whitespace', () => {
    const search = createSearch();
    assert.deepEqual(search('  BLU   bott  ').visible, [true, false, false]);
    assert.deepEqual(search('free RED').visible, [false, false, true]);
    assert.deepEqual(search('size 2').visible, [false, true, false]);
});

test('shows no matches and restores all products when cleared', () => {
    const search = createSearch();
    assert.deepEqual(search('unknown'), {
        visible: [false, false, false], noMatches: true,
    });
    assert.deepEqual(search(''), {
        visible: [true, true, true], noMatches: false,
    });
    assert.deepEqual(search('   '), {
        visible: [true, true, true], noMatches: false,
    });
});
