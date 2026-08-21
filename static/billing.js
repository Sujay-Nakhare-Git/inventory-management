// Billing cart state
let cartItems = [];
let currentStoreCredit = null;  // Store current credit info
let pendingCustomerCredit = null;  // Credit info detected from customer autocomplete

function switchBillingTab(type) {
    const isRental = type === 'rental';
    document.getElementById('salesBillingPanel').hidden = isRental;
    document.getElementById('rentalBillingPanel').hidden = !isRental;
    document.getElementById('salesTab').classList.toggle('active', !isRental);
    document.getElementById('rentalsTab').classList.toggle('active', isRental);
    document.getElementById('salesTab').setAttribute('aria-selected', String(!isRental));
    document.getElementById('rentalsTab').setAttribute('aria-selected', String(isRental));
}

function updateRentalTotal() {
    const rentalDays = parseInt(document.getElementById('rentalDays').value, 10) || 0;
    const rentAmount = parseFloat(document.getElementById('rentAmount').value) || 0;
    const depositAmount = parseFloat(document.getElementById('depositAmount').value) || 0;
    const rentalCharges = rentalDays * rentAmount;
    document.getElementById('rentalCharges').textContent = `₹${rentalCharges.toFixed(2)}`;
    document.getElementById('rentalTotalAmount').textContent = `₹${(rentalCharges + depositAmount).toFixed(2)}`;
}

function round2(value) {
    return Math.round((Number(value) + Number.EPSILON) * 100) / 100;
}

function getSelectedPaymentMode() {
    const selected = document.querySelector('input[name="paymentMode"]:checked');
    return selected ? selected.value : 'single';
}

function togglePaymentMode() {
    const mode = getSelectedPaymentMode();
    document.getElementById('singlePaymentSection').style.display = mode === 'single' ? 'block' : 'none';
    document.getElementById('splitPaymentSection').style.display = mode === 'split' ? 'block' : 'none';
    recalculate();
}

// Product search filter
document.getElementById('productSearch').addEventListener('input', function() {
    const query = this.value.toLowerCase();
    document.querySelectorAll('.product-item').forEach(item => {
        const searchText = item.dataset.search;
        item.style.display = searchText.includes(query) ? 'flex' : 'none';
    });
});

// ---------- Customer autocomplete ----------
let customerSearchTimer = null;
let customerSearchSeq = 0;

function escapeHtml(str) {
    return String(str || '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[c]));
}

async function fetchCustomerSuggestions(query) {
    const seq = ++customerSearchSeq;
    const box = document.getElementById('customerSuggestions');
    if (!query || query.length < 2) {
        box.style.display = 'none';
        box.innerHTML = '';
        return;
    }
    try {
        const resp = await fetch(`/api/customers/search?q=${encodeURIComponent(query)}`);
        if (!resp.ok) return;
        const data = await resp.json();
        if (seq !== customerSearchSeq) return; // stale
        renderCustomerSuggestions(data);
    } catch (e) {
        // silent fail
    }
}

function renderCustomerSuggestions(results) {
    const box = document.getElementById('customerSuggestions');
    if (!results || results.length === 0) {
        box.style.display = 'none';
        box.innerHTML = '';
        return;
    }
    box.innerHTML = results.map((c, idx) => {
        const credit = c.credit_balance > 0
            ? `<span class="cs-credit">💳 ₹${Number(c.credit_balance).toFixed(2)}</span>`
            : '';
        const visits = c.visit_count > 1 ? `${c.visit_count} visits` : 'Repeat customer';
        return `
            <div class="customer-suggestion-item" data-idx="${idx}">
                <div class="cs-name">${escapeHtml(c.name) || '(no name)'}</div>
                <div class="cs-meta">
                    <span>${escapeHtml(c.phone)} · ${visits}</span>
                    ${credit}
                </div>
            </div>`;
    }).join('');
    box.style.display = 'block';

    box.querySelectorAll('.customer-suggestion-item').forEach(el => {
        el.addEventListener('mousedown', (ev) => {
            ev.preventDefault();
            const idx = parseInt(el.dataset.idx);
            selectCustomer(results[idx]);
        });
    });
}

function selectCustomer(customer) {
    document.getElementById('customerName').value = customer.name || '';
    document.getElementById('customerPhone').value = customer.phone || '';
    document.getElementById('customerSuggestions').style.display = 'none';

    const banner = document.getElementById('customerCreditBanner');
    if (customer.credit_id && customer.credit_balance > 0) {
        pendingCustomerCredit = {
            id: customer.credit_id,
            customer_name: customer.name,
            customer_phone: customer.phone,
            balance: customer.credit_balance,
        };
        document.getElementById('customerCreditBalanceText').textContent =
            `₹${Number(customer.credit_balance).toFixed(2)}`;
        banner.style.display = 'block';
    } else {
        pendingCustomerCredit = null;
        banner.style.display = 'none';
    }
}

function applyCustomerStoreCredit() {
    if (!pendingCustomerCredit) return;
    const checkbox = document.getElementById('useStoreCredit');
    checkbox.checked = true;
    document.getElementById('storeCreditFields').style.display = 'block';
    document.getElementById('storeCreditPhone').value = pendingCustomerCredit.customer_phone;

    currentStoreCredit = pendingCustomerCredit;
    document.getElementById('storeCreditCustomerName').textContent = pendingCustomerCredit.customer_name || '—';
    document.getElementById('storeCreditBalance').textContent = `₹${pendingCustomerCredit.balance.toFixed(2)}`;
    document.getElementById('storeCreditAmount').max = pendingCustomerCredit.balance;
    document.getElementById('storeCreditAmount').value = '0';
    document.getElementById('storeCreditInfo').style.display = 'block';
    document.getElementById('storeCreditError').style.display = 'none';
    document.getElementById('customerCreditBanner').style.display = 'none';
    recalculate();
}

function bindCustomerAutocomplete() {
    const nameEl = document.getElementById('customerName');
    const phoneEl = document.getElementById('customerPhone');
    const trigger = (val) => {
        clearTimeout(customerSearchTimer);
        customerSearchTimer = setTimeout(() => fetchCustomerSuggestions(val.trim()), 200);
    };
    nameEl.addEventListener('input', (e) => trigger(e.target.value));
    phoneEl.addEventListener('input', (e) => trigger(e.target.value));

    document.addEventListener('click', (e) => {
        if (!e.target.closest('.customer-fields')) {
            document.getElementById('customerSuggestions').style.display = 'none';
        }
    });
}

bindCustomerAutocomplete();
// ---------- /Customer autocomplete ----------

document.getElementById('rentalDays').addEventListener('input', updateRentalTotal);
document.getElementById('rentAmount').addEventListener('input', updateRentalTotal);
document.getElementById('depositAmount').addEventListener('input', updateRentalTotal);

function toggleStoreCredit() {
    const useCredit = document.getElementById('useStoreCredit').checked;
    document.getElementById('storeCreditFields').style.display = useCredit ? 'block' : 'none';
    if (!useCredit) {
        currentStoreCredit = null;
        document.getElementById('storeCreditInfo').style.display = 'none';
        document.getElementById('storeCreditAmount').value = '0';
        recalculate();
    }
}

async function lookupStoreCredit() {
    const phone = document.getElementById('storeCreditPhone').value.trim();
    const errorDiv = document.getElementById('storeCreditError');
    
    if (!phone || phone.length !== 10) {
        errorDiv.textContent = 'Please enter a valid 10-digit phone number.';
        errorDiv.style.display = 'block';
        return;
    }

    try {
        const resp = await fetch(`/api/store-credit/lookup/${phone}`);
        const data = await resp.json();
        
        if (!data.found) {
            errorDiv.textContent = 'No store credit account found for this phone number.';
            errorDiv.style.display = 'block';
            document.getElementById('storeCreditInfo').style.display = 'none';
            currentStoreCredit = null;
            return;
        }

        errorDiv.style.display = 'none';
        currentStoreCredit = data;
        document.getElementById('storeCreditCustomerName').textContent = data.customer_name;
        document.getElementById('storeCreditBalance').textContent = `₹${data.balance.toFixed(2)}`;
        document.getElementById('storeCreditAmount').max = data.balance;
        document.getElementById('storeCreditAmount').value = '0';
        document.getElementById('storeCreditInfo').style.display = 'block';
        recalculate();
    } catch (err) {
        errorDiv.textContent = 'Error looking up store credit. Please try again.';
        errorDiv.style.display = 'block';
    }
}

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

    const pct = parseFloat(document.getElementById('discountPercentInput').value) || 0;
    if (pct > 0) {
        document.getElementById('discountAmountInput').value = round2(getCartSubtotal() * pct / 100);
    }
    recalculate();
}

function getCartSubtotal() {
    return cartItems.reduce((sum, i) => sum + i.total_price, 0);
}

function onDiscountAmountChange() {
    const subtotal = getCartSubtotal();
    const amt = parseFloat(document.getElementById('discountAmountInput').value) || 0;
    const pct = subtotal > 0 ? round2((Math.min(amt, subtotal) / subtotal) * 100) : 0;
    document.getElementById('discountPercentInput').value = pct;
    recalculate();
}

function onDiscountPercentChange() {
    const subtotal = getCartSubtotal();
    const pct = Math.min(Math.max(0, parseFloat(document.getElementById('discountPercentInput').value) || 0), 100);
    document.getElementById('discountPercentInput').value = pct;
    const amt = round2(subtotal * pct / 100);
    document.getElementById('discountAmountInput').value = amt;
    recalculate();
}

function calculateTotals() {
    const subtotal = getCartSubtotal();
    const discountInput = parseFloat(document.getElementById('discountAmountInput').value) || 0;
    const taxPct = parseFloat(document.getElementById('taxPercent').value) || 0;

    const discountAmt = round2(Math.min(Math.max(0, discountInput), subtotal));
    const afterDiscount = subtotal - discountAmt;
    const taxAmt = round2(afterDiscount * taxPct / 100);

    let storeCreditAmt = 0;
    if (document.getElementById('useStoreCredit').checked && currentStoreCredit) {
        storeCreditAmt = parseFloat(document.getElementById('storeCreditAmount').value) || 0;
        storeCreditAmt = Math.min(storeCreditAmt, currentStoreCredit.balance);
    }

    const total = Math.max(0, round2(afterDiscount + taxAmt - storeCreditAmt));
    return { subtotal, discountAmt, taxPct, taxAmt, storeCreditAmt, total };
}

function getPaymentBreakdownPayload(totalAmount) {
    const mode = getSelectedPaymentMode();
    if (totalAmount <= 0) {
        return {
            payment_method: 'Store Credit',
            payment_breakdown: []
        };
    }

    if (mode === 'single') {
        const method = document.getElementById('paymentMethod').value;
        return {
            payment_method: method,
            payment_breakdown: [{ method: method, amount: round2(totalAmount) }]
        };
    }

    const splitRows = [
        { method: 'Cash', amount: parseFloat(document.getElementById('payCash').value) || 0 },
        { method: 'UPI', amount: parseFloat(document.getElementById('payUPI').value) || 0 },
        { method: 'Card', amount: parseFloat(document.getElementById('payCard').value) || 0 },
        { method: 'Bank Transfer', amount: parseFloat(document.getElementById('payBankTransfer').value) || 0 }
    ].map(row => ({ method: row.method, amount: round2(Math.max(0, row.amount)) }))
     .filter(row => row.amount > 0);

    const splitTotal = round2(splitRows.reduce((sum, row) => sum + row.amount, 0));
    if (splitRows.length === 0) {
        throw new Error('Add at least one split payment amount.');
    }
    if (Math.abs(splitTotal - round2(totalAmount)) > 0.05) {
        throw new Error('Split payment total must match bill total.');
    }

    return {
        payment_method: splitRows.length > 1 ? 'Mixed' : splitRows[0].method,
        payment_breakdown: splitRows
    };
}

function recalculate() {
    const totals = calculateTotals();

    document.getElementById('subtotal').textContent = `₹${totals.subtotal.toFixed(2)}`;
    document.getElementById('discountAmount').textContent = `-₹${totals.discountAmt.toFixed(2)}`;
    document.getElementById('taxAmount').textContent = `+₹${totals.taxAmt.toFixed(2)}`;
    document.getElementById('totalAmount').textContent = `₹${totals.total.toFixed(2)}`;
    document.getElementById('storeCreditUsed').textContent = `-₹${totals.storeCreditAmt.toFixed(2)}`;

    document.getElementById('discountRow').style.display = totals.discountAmt > 0 ? 'flex' : 'none';
    document.getElementById('taxRow').style.display = totals.taxPct > 0 ? 'flex' : 'none';
    document.getElementById('storeCreditRow').style.display = totals.storeCreditAmt > 0 ? 'flex' : 'none';

    if (getSelectedPaymentMode() === 'split') {
        const splitTotal = round2(
            (parseFloat(document.getElementById('payCash').value) || 0) +
            (parseFloat(document.getElementById('payUPI').value) || 0) +
            (parseFloat(document.getElementById('payCard').value) || 0) +
            (parseFloat(document.getElementById('payBankTransfer').value) || 0)
        );
        const remaining = round2(totals.total - splitTotal);
        const hint = document.getElementById('splitPaymentHint');
        hint.textContent = `Remaining: ₹${remaining.toFixed(2)}`;
        hint.style.color = Math.abs(remaining) <= 0.05 ? '#16a34a' : '#b45309';
    }
}

async function submitBill() {
    if (cartItems.length === 0) {
        alert('Add at least one product to the bill.');
        return;
    }

    const customerNameEl = document.getElementById('customerName');
    const customerPhoneEl = document.getElementById('customerPhone');
    const customerName = customerNameEl.value.trim();
    const customerPhone = customerPhoneEl.value.trim();

    if (!customerName || !customerPhone) {
        alert('Customer name and phone number are required before generating a bill.');
        if (!customerName) {
            customerNameEl.focus();
        } else {
            customerPhoneEl.focus();
        }
        return;
    }

    // Open early so browsers treat it as a direct user-initiated popup.
    const printWindow = window.open('', '_blank');

    const storeCreditAmt = document.getElementById('useStoreCredit').checked && currentStoreCredit 
        ? (parseFloat(document.getElementById('storeCreditAmount').value) || 0) 
        : 0;

    const totals = calculateTotals();
    let paymentPayload;
    try {
        paymentPayload = getPaymentBreakdownPayload(totals.total);
    } catch (err) {
        alert(err.message || 'Please fix payment breakup.');
        return;
    }

    const payload = {
        customer_name: customerName,
        customer_phone: customerPhone,
        discount_amount: parseFloat(document.getElementById('discountAmountInput').value) || 0,
        tax_percent: parseFloat(document.getElementById('taxPercent').value) || 0,
        payment_method: paymentPayload.payment_method,
        payment_breakdown: paymentPayload.payment_breakdown,
        store_credit_id: currentStoreCredit ? currentStoreCredit.id : null,
        store_credit_amount: storeCreditAmt,
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
            if (printWindow && !printWindow.closed) {
                printWindow.close();
            }
            alert(data.error || 'Failed to create bill.');
            return;
        }

        const billRef = data.bill_number || `#${data.bill_id}`;
        const thermalUrl = `/bills/${data.bill_id}/thermal`;

        if (printWindow && !printWindow.closed) {
            printWindow.location.href = thermalUrl;
        }

        document.getElementById('billMessage').textContent =
            `Bill ${billRef} created — Total: ₹${data.total.toFixed(2)}.`;
        document.getElementById('viewBillLink').href = `/bills/${data.bill_id}`;
        document.getElementById('billModal').style.display = 'flex';

        // Fallback when popup gets blocked by browser settings.
        if (!printWindow) {
            window.open(thermalUrl, '_blank');
        }

        cartItems = [];
        currentStoreCredit = null;
        pendingCustomerCredit = null;
        document.getElementById('customerCreditBanner').style.display = 'none';
        document.getElementById('useStoreCredit').checked = false;
        document.querySelector('input[name="paymentMode"][value="single"]').checked = true;
        document.getElementById('payCash').value = '0';
        document.getElementById('payUPI').value = '0';
        document.getElementById('payCard').value = '0';
        document.getElementById('payBankTransfer').value = '0';
        togglePaymentMode();
        toggleStoreCredit();
    } catch (err) {
        if (printWindow && !printWindow.closed) {
            printWindow.close();
        }
        alert('Network error. Please try again.');
    }
}

async function submitRentalBill() {
    const nameEl = document.getElementById('rentalName');
    const phoneEl = document.getElementById('rentalPhone');
    const daysEl = document.getElementById('rentalDays');
    const rentEl = document.getElementById('rentAmount');
    const depositEl = document.getElementById('depositAmount');
    const customerName = nameEl.value.trim();
    const customerPhone = phoneEl.value.trim();
    const rentalDays = parseInt(daysEl.value, 10);
    const rentAmount = parseFloat(rentEl.value);
    const depositAmount = parseFloat(depositEl.value);

    if (!customerName || !customerPhone) {
        alert('Name and phone number are required before generating a rental bill.');
        (!customerName ? nameEl : phoneEl).focus();
        return;
    }
    if (!Number.isInteger(rentalDays) || rentalDays <= 0) {
        alert('Please enter at least 1 rental day.');
        daysEl.focus();
        return;
    }
    if (!Number.isFinite(rentAmount) || rentAmount <= 0) {
        alert('Please enter a rent amount greater than ₹0.');
        rentEl.focus();
        return;
    }
    if (!Number.isFinite(depositAmount) || depositAmount < 0) {
        alert('Please enter a valid deposit amount.');
        depositEl.focus();
        return;
    }

    const rentalCharges = round2(rentalDays * rentAmount);
    const total = round2(rentalCharges + depositAmount);
    const printWindow = window.open('', '_blank');

    try {
        const resp = await fetch('/api/billing', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                bill_type: 'rental',
                customer_name: customerName,
                customer_phone: customerPhone,
                rental_days: rentalDays,
                rent_amount: round2(rentAmount),
                deposit_amount: round2(depositAmount),
                payment_method: 'Cash',
                payment_breakdown: [{ method: 'Cash', amount: total }]
            })
        });

        const data = await resp.json();
        if (!resp.ok) {
            if (printWindow && !printWindow.closed) printWindow.close();
            alert(data.error || 'Failed to create rental bill.');
            return;
        }

        const billRef = data.bill_number || `#${data.bill_id}`;
        const thermalUrl = `/bills/${data.bill_id}/thermal`;
        if (printWindow && !printWindow.closed) printWindow.location.href = thermalUrl;
        document.getElementById('billMessage').textContent =
            `Rental bill ${billRef} created — Rental charges: ₹${rentalCharges.toFixed(2)}; total payable: ₹${data.total.toFixed(2)}.`;
        document.getElementById('viewBillLink').href = `/bills/${data.bill_id}`;
        document.getElementById('billModal').style.display = 'flex';
        if (!printWindow) window.open(thermalUrl, '_blank');

        nameEl.value = '';
        phoneEl.value = '';
        daysEl.value = '1';
        rentEl.value = '';
        depositEl.value = '';
        updateRentalTotal();
    } catch (err) {
        if (printWindow && !printWindow.closed) printWindow.close();
        alert('Network error. Please try again.');
    }
}
