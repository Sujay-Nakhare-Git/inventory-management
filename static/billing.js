// Billing cart state
let cartItems = [];

// Product search filter
document.getElementById('productSearch').addEventListener('input', function() {
    const query = this.value.toLowerCase();
    document.querySelectorAll('.product-item').forEach(item => {
        const searchText = item.dataset.search;
        item.style.display = searchText.includes(query) ? 'flex' : 'none';
    });
});

function addItem(id, name, price, maxStock) {
    const existing = cartItems.find(i => i.product_id === id);
    if (existing) {
        if (existing.quantity >= maxStock) {
            alert(`Maximum stock (${maxStock}) reached for "${name}".`);
            return;
        }
        existing.quantity++;
        existing.total_price = existing.quantity * existing.unit_price;
    } else {
        cartItems.push({
            product_id: id,
            name: name,
            unit_price: price,
            quantity: 1,
            total_price: price,
            max_stock: maxStock
        });
    }
    renderCart();
}

function removeItem(index) {
    cartItems.splice(index, 1);
    renderCart();
}

function updateQuantity(index, qty) {
    qty = parseInt(qty);
    if (qty <= 0) {
        removeItem(index);
        return;
    }
    if (qty > cartItems[index].max_stock) {
        alert(`Maximum stock is ${cartItems[index].max_stock}.`);
        qty = cartItems[index].max_stock;
    }
    cartItems[index].quantity = qty;
    cartItems[index].total_price = qty * cartItems[index].unit_price;
    renderCart();
}

function renderCart() {
    const container = document.getElementById('billItems');
    const summary = document.getElementById('billSummary');
    const emptyMsg = document.getElementById('emptyCart');

    if (cartItems.length === 0) {
        container.innerHTML = '<p class="empty-state" id="emptyCart">Add products to start billing</p>';
        summary.style.display = 'none';
        return;
    }

    let html = '';
    cartItems.forEach((item, idx) => {
        html += `
        <div class="bill-item">
            <span class="item-name">${item.name}</span>
            <input type="number" class="input item-qty" value="${item.quantity}"
                   min="1" max="${item.max_stock}"
                   onchange="updateQuantity(${idx}, this.value)">
            <span>× ₹${item.unit_price.toFixed(2)}</span>
            <span class="item-total">₹${item.total_price.toFixed(2)}</span>
            <button class="remove-btn" onclick="removeItem(${idx})">✕</button>
        </div>`;
    });
    container.innerHTML = html;
    summary.style.display = 'block';
    recalculate();
}

function recalculate() {
    const subtotal = cartItems.reduce((sum, i) => sum + i.total_price, 0);
    const discountPct = parseFloat(document.getElementById('discountPercent').value) || 0;
    const taxPct = parseFloat(document.getElementById('taxPercent').value) || 0;

    const discountAmt = subtotal * discountPct / 100;
    const afterDiscount = subtotal - discountAmt;
    const taxAmt = afterDiscount * taxPct / 100;
    const total = afterDiscount + taxAmt;

    document.getElementById('subtotal').textContent = `₹${subtotal.toFixed(2)}`;
    document.getElementById('discountAmount').textContent = `-₹${discountAmt.toFixed(2)}`;
    document.getElementById('taxAmount').textContent = `+₹${taxAmt.toFixed(2)}`;
    document.getElementById('totalAmount').textContent = `₹${total.toFixed(2)}`;

    document.getElementById('discountRow').style.display = discountPct > 0 ? 'flex' : 'none';
    document.getElementById('taxRow').style.display = taxPct > 0 ? 'flex' : 'none';
}

async function submitBill() {
    if (cartItems.length === 0) {
        alert('Add at least one product to the bill.');
        return;
    }

    const payload = {
        customer_name: document.getElementById('customerName').value.trim(),
        customer_phone: document.getElementById('customerPhone').value.trim(),
        discount_percent: parseFloat(document.getElementById('discountPercent').value) || 0,
        tax_percent: parseFloat(document.getElementById('taxPercent').value) || 0,
        payment_method: document.getElementById('paymentMethod').value,
        items: cartItems.map(i => ({
            product_id: i.product_id,
            quantity: i.quantity
        }))
    };

    try {
        const resp = await fetch('/api/billing', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await resp.json();
        if (!resp.ok) {
            alert(data.error || 'Failed to create bill.');
            return;
        }

        document.getElementById('billMessage').textContent =
            `Bill #${data.bill_id} created — Total: ₹${data.total.toFixed(2)}`;
        document.getElementById('viewBillLink').href = `/bills/${data.bill_id}`;
        document.getElementById('billModal').style.display = 'flex';
        cartItems = [];
    } catch (err) {
        alert('Network error. Please try again.');
    }
}
