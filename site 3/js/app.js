/* ================================================================
   Fichas Técnicas — multi-tenant SPA (Firebase + vanilla JS)
   ================================================================ */

import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js';
import {
  getFirestore, collection, doc, setDoc, deleteDoc, onSnapshot,
  getDoc, getDocs, writeBatch, collectionGroup, serverTimestamp
} from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js';
import {
  getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword,
  sendPasswordResetEmail, onAuthStateChanged, signOut
} from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js';

// ---------- Firebase config ----------
const firebaseConfig = {
  apiKey: "AIzaSyDF3jKFty1lQrib9lwsRoRxBFTsO-2boMY",
  authDomain: "fichastecnicas-c3829.firebaseapp.com",
  projectId: "fichastecnicas-c3829",
  storageBucket: "fichastecnicas-c3829.firebasestorage.app",
  messagingSenderId: "461159721803",
  appId: "1:461159721803:web:922a9a8a39f4c5ec2416ce"
};
const MASTER_EMAIL = "gustavo@quibebe.com.br";

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);
const auth = getAuth(firebaseApp);

// ---------- State ----------
const STATE = {
  user: null,           // Firebase auth user
  userDoc: null,        // { role, clienteIds, email, name }
  currentClienteId: null,
  currentCliente: null, // { id, name, ... }
  clientes: [],         // list of accessible clientes (for master/staff picker)
  dishes: [],
  insumos: [],
  equipamentos: [],
  loaded: false,
  unsubs: { dishes: null, insumos: null, config: null, clientes: null }
};

// ---------- Utilities ----------
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const el = (tag, attrs = {}, ...children) => {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === 'class') node.className = v;
    else if (k === 'html') node.innerHTML = v;
    else if (k.startsWith('on')) node.addEventListener(k.slice(2).toLowerCase(), v);
    else if (v === true) node.setAttribute(k, '');
    else if (v !== false && v != null) node.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c == null || c === false) continue;
    node.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
  }
  return node;
};
const fmtBRL = v => (typeof v === 'number' && isFinite(v)) ? v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' }) : 'R$ 0,00';
const fmtNum = (v, d = 2) => (typeof v === 'number' && isFinite(v)) ? v.toLocaleString('pt-BR', { minimumFractionDigits: d, maximumFractionDigits: d }) : '—';

// Normaliza quantidade para kg (peso) ou l (volume) com 3 decimais; outros permanecem.
const WEIGHT_G = ['g','gr','gram','grama','gramas'];
const WEIGHT_MG = ['mg'];
const WEIGHT_KG = ['kg','quilo','quilos','quilograma','quilogramas'];
const VOL_ML = ['ml','millilitro','mililitro'];
const VOL_L = ['l','lt','litro','litros'];
function normUnitForDisplay(qty, unit) {
  if (qty == null) return { qty: null, unit: unit || '', text: '—' };
  const u = (unit || '').toLowerCase().trim().replace(/\s+/g, '');
  if (WEIGHT_G.includes(u)) return { qty: qty/1000, unit: 'kg', text: fmtNum(qty/1000, 3) };
  if (WEIGHT_MG.includes(u)) return { qty: qty/1e6, unit: 'kg', text: fmtNum(qty/1e6, 3) };
  if (WEIGHT_KG.includes(u)) return { qty, unit: 'kg', text: fmtNum(qty, 3) };
  if (VOL_ML.includes(u)) return { qty: qty/1000, unit: 'l', text: fmtNum(qty/1000, 3) };
  if (VOL_L.includes(u)) return { qty, unit: 'l', text: fmtNum(qty, 3) };
  const dec = qty % 1 === 0 ? 0 : 2;
  return { qty, unit: unit || '', text: fmtNum(qty, dec) };
}
// Returns formatted "qty unit" string or "Q.B" / raw fallback
function formatIngQty(ing, scaleFactor = 1) {
  if (ing.is_qb) return { text: 'Q.B', unit: '' };
  if (ing.qty == null) return { text: ing.qty_raw || '—', unit: ing.unit || '' };
  const scaled = ing.qty * (scaleFactor || 1);
  const n = normUnitForDisplay(scaled, ing.unit);
  return { text: n.text, unit: n.unit };
}
// Convert insumo price to normalized per-unit display (per kg, per l, or as-is)
function normalizePriceForDisplay(insumo) {
  if (!insumo) return null;
  const u = (insumo.unit || '').toLowerCase().trim();
  const p = insumo.price || 0;
  if (WEIGHT_G.includes(u)) return { price: p * 1000, unit: 'kg' };
  if (WEIGHT_MG.includes(u)) return { price: p * 1e6, unit: 'kg' };
  if (WEIGHT_KG.includes(u)) return { price: p, unit: 'kg' };
  if (VOL_ML.includes(u)) return { price: p * 1000, unit: 'l' };
  if (VOL_L.includes(u)) return { price: p, unit: 'l' };
  return { price: p, unit: insumo.unit || '—' };
}

// Normalize rendimento string (just leading number + unit) — preserves original case
function formatRendimento(rendStr, scaleFactor = 1) {
  if (!rendStr) return '';
  const s = String(rendStr).trim();
  const m = s.match(/^(\d+(?:[.,]\d+)?)\s*([a-zA-Zµ\/\u00c0-\u024f]*)(.*)$/);
  if (!m) return rendStr;
  const rawQty = parseFloat(m[1].replace(',', '.'));
  const originalUnit = m[2] || '';
  const qty = rawQty * (scaleFactor || 1);
  const n = normUnitForDisplay(qty, originalUnit);
  // Se o unit foi convertido (ex: 'g' → 'kg'), usa a versão curta; senão preserva o case original
  const unitChanged = n.unit.toLowerCase() !== originalUnit.toLowerCase();
  const unitDisplay = unitChanged ? n.unit : originalUnit;
  const suffix = m[3].trim();
  return `${n.text} ${unitDisplay}${suffix ? ' ' + suffix : ''}`.trim();
}
const slugify = s => (s || '').toString().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'item';
const uid = () => Math.random().toString(36).slice(2, 9);

function toast(msg) {
  const t = $('#toast');
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(t._to);
  t._to = setTimeout(() => { t.hidden = true; }, 2600);
}

// ---------- Debouncing ----------
const pendingWrites = new Map();
function scheduleSave(key, fn, delay = 700) {
  const existing = pendingWrites.get(key);
  if (existing) clearTimeout(existing);
  pendingWrites.set(key, setTimeout(async () => {
    pendingWrites.delete(key);
    try { await fn(); } catch (e) { console.error(e); toast('Erro: ' + e.message); }
  }, delay));
}

// ---------- Permission helpers ----------
const isMaster = () => STATE.userDoc && (STATE.userDoc.role === 'master' || STATE.user?.email === MASTER_EMAIL);
const isStaff = () => STATE.userDoc && STATE.userDoc.role === 'staff';
const isClienteUser = () => STATE.userDoc && STATE.userDoc.role === 'cliente';
const canEditCliente = (cid) => isMaster() || (isStaff() && (STATE.userDoc.clienteIds || []).includes(cid));
const canViewCliente = (cid) => isMaster() || (STATE.userDoc?.clienteIds || []).includes(cid);
const canEditInsumoPrice = (cid) => canEditCliente(cid) || (isClienteUser() && (STATE.userDoc.clienteIds || []).includes(cid));
const canManageUsers = () => isMaster();
const canManageClientes = () => isMaster();

// ---------- Firestore paths ----------
const dishesCol = (cid) => collection(db, 'clientes', cid, 'dishes');
const dishDoc = (cid, did) => doc(db, 'clientes', cid, 'dishes', did);
const insumosCol = (cid) => collection(db, 'clientes', cid, 'insumos');
const insumoDoc = (cid, iid) => doc(db, 'clientes', cid, 'insumos', iid);
const configDoc = (cid, name) => doc(db, 'clientes', cid, 'config', name);
const clienteDoc = (cid) => doc(db, 'clientes', cid);

// ---------- Firestore writes ----------
async function saveDish(cid, dish) {
  const clean = { ...dish };
  delete clean.id;
  await setDoc(dishDoc(cid, dish.id), clean);
}
async function deleteDish(cid, did) {
  await deleteDoc(dishDoc(cid, did));
}
async function saveInsumo(cid, insumo) {
  const clean = { ...insumo };
  delete clean.id;
  await setDoc(insumoDoc(cid, insumo.id), clean, { merge: true });
}
async function deleteInsumo(cid, iid) {
  await deleteDoc(insumoDoc(cid, iid));
}
async function saveCliente(cliente) {
  const clean = { ...cliente };
  delete clean.id;
  await setDoc(clienteDoc(cliente.id), clean, { merge: true });
}
async function deleteCliente(cid) {
  // Warning: doesn't cascade delete subcollections (Firestore limitation in client SDK).
  // For MVP: delete just the cliente doc; user should be warned.
  await deleteDoc(clienteDoc(cid));
}

// ---------- Real-time listeners ----------
function clearListeners() {
  Object.entries(STATE.unsubs).forEach(([k, u]) => { if (u) { u(); STATE.unsubs[k] = null; } });
  STATE.dishes = []; STATE.insumos = []; STATE.equipamentos = []; STATE.currentCliente = null;
  STATE.loaded = false;
}

function subscribeCliente(cid) {
  clearListeners();
  STATE.currentClienteId = cid;
  let pendingLoads = 4;
  const maybeRender = () => {
    if (--pendingLoads <= 0) { STATE.loaded = true; route(); }
  };
  // ⚠ Após o load inicial, não chamamos route() mais a cada snapshot.
  // O STATE já é atualizado; re-renderizar perdia foco em inputs (preços, markup, etc).
  // Cliente doc
  STATE.unsubs.clienteDoc = onSnapshot(clienteDoc(cid), (snap) => {
    if (snap.exists()) STATE.currentCliente = { id: snap.id, ...snap.data() };
    else STATE.currentCliente = null;
    if (pendingLoads > 0) maybeRender();
  });
  // Dishes
  STATE.unsubs.dishes = onSnapshot(dishesCol(cid), (snap) => {
    STATE.dishes = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    STATE.dishes.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    if (pendingLoads > 0) maybeRender();
  }, err => { console.error('dishes err', err); if (pendingLoads > 0) maybeRender(); });
  // Insumos
  STATE.unsubs.insumos = onSnapshot(insumosCol(cid), (snap) => {
    STATE.insumos = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    STATE.insumos.sort((a, b) => (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase()));
    if (pendingLoads > 0) maybeRender();
  }, err => { console.error('insumos err', err); if (pendingLoads > 0) maybeRender(); });
  // Equipamentos
  STATE.unsubs.config = onSnapshot(configDoc(cid, 'equipamentos'), (snap) => {
    STATE.equipamentos = snap.exists() ? (snap.data().list || []) : [];
    if (pendingLoads > 0) maybeRender();
  }, err => { console.error('config err', err); if (pendingLoads > 0) maybeRender(); });
}

async function loadClientesList() {
  try {
    if (isMaster()) {
      // Master: lista toda a coleção
      const snap = await getDocs(collection(db, 'clientes'));
      STATE.clientes = snap.docs.map(d => ({ id: d.id, ...d.data() }))
        .sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    } else {
      // Staff/cliente: busca apenas os que estão em clienteIds (regras bloqueiam collection query)
      const ids = STATE.userDoc?.clienteIds || [];
      if (ids.length === 0) { STATE.clientes = []; return; }
      const docs = await Promise.all(ids.map(id => getDoc(doc(db, 'clientes', id))));
      STATE.clientes = docs
        .filter(d => d.exists())
        .map(d => ({ id: d.id, ...d.data() }))
        .sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    }
  } catch (err) {
    console.error('loadClientesList error:', err);
    STATE.clientes = [];
    toast('Erro ao listar restaurantes: ' + err.message);
  }
}

// ---------- Sync de preços apenas (sem mexer em fichas ou outros campos) ----------
async function syncPricesOnly(cid) {
  if (!isMaster() && !isStaff()) { toast('Sem permissão'); return; }
  const msg = 'Atualizar preços dos insumos deste restaurante usando os valores do arquivo data.json?\n\n• SEGURO: só altera preços. Não mexe em fichas, não adiciona nem remove insumos.\n• Preços existentes > 0 são sobrescritos pelos do arquivo.';
  if (!confirm(msg)) return;
  try {
    toast('Sincronizando preços...');
    const resp = await fetch('data/data.json');
    const seed = await resp.json();
    const seedPrices = {};
    for (const ins of (seed.insumos || [])) {
      if (typeof ins.price === 'number' && ins.price > 0) seedPrices[ins.id] = ins.price;
    }
    const snap = await getDocs(insumosCol(cid));
    let batch = writeBatch(db);
    let count = 0, updated = 0;
    for (const d of snap.docs) {
      if (seedPrices[d.id] != null) {
        batch.update(insumoDoc(cid, d.id), { price: seedPrices[d.id] });
        updated++;
        count++;
        if (count >= 400) { await batch.commit(); batch = writeBatch(db); count = 0; }
      }
    }
    if (count > 0) await batch.commit();
    toast(`✓ ${updated} preços atualizados`);
  } catch (err) {
    console.error(err);
    toast('Erro ao sincronizar: ' + err.message);
  }
}

// ---------- Seed / migration ----------
async function seedClienteFromJson(cid, clienteName) {
  toast('Importando dados do arquivo inicial...');
  const resp = await fetch('data/data.json');
  const seed = await resp.json();

  // Ensure cliente doc
  await setDoc(clienteDoc(cid), { name: clienteName || cid, slug: cid, createdAt: serverTimestamp() }, { merge: true });

  // Keep existing prices
  const existingPricesSnap = await getDocs(insumosCol(cid));
  const existingPrices = {};
  existingPricesSnap.docs.forEach(d => { existingPrices[d.id] = d.data().price || 0; });

  // Delete orphans from dishes + insumos (exist but not in seed)
  const existingDishes = await getDocs(dishesCol(cid));
  const seedDishIds = new Set(seed.dishes.map(d => d.id));
  const seedInsumoIds = new Set(seed.insumos.map(i => i.id));
  let batch = writeBatch(db);
  let count = 0;
  const flush = async () => { if (count > 0) { await batch.commit(); batch = writeBatch(db); count = 0; } };
  const add = async (op, ref, data) => {
    if (op === 'set') batch.set(ref, data);
    else batch.delete(ref);
    count++;
    if (count >= 400) await flush();
  };
  for (const d of existingDishes.docs) if (!seedDishIds.has(d.id)) await add('delete', dishDoc(cid, d.id));
  for (const d of existingPricesSnap.docs) if (!seedInsumoIds.has(d.id)) await add('delete', insumoDoc(cid, d.id));
  for (const dish of seed.dishes) {
    const clean = { ...dish };
    delete clean.id;
    await add('set', dishDoc(cid, dish.id), clean);
  }
  for (const ins of seed.insumos) {
    const clean = { ...ins };
    delete clean.id;
    if (existingPrices[ins.id] != null && existingPrices[ins.id] > 0) clean.price = existingPrices[ins.id];
    await add('set', insumoDoc(cid, ins.id), clean);
  }
  await add('set', configDoc(cid, 'equipamentos'), { list: seed.equipamentos_disponiveis || [] });
  await flush();
  toast('Importação concluída!');
}

// ---------- Auth ----------
async function ensureUserDoc(user) {
  const ref = doc(db, 'users', user.uid);
  const snap = await getDoc(ref);
  if (snap.exists()) { STATE.userDoc = { uid: user.uid, ...snap.data() }; return; }
  // Bootstrap: if matches master email, auto-create as master
  if (user.email === MASTER_EMAIL) {
    const d = { email: user.email, role: 'master', clienteIds: [], name: 'Master', createdAt: serverTimestamp() };
    await setDoc(ref, d);
    STATE.userDoc = { uid: user.uid, ...d };
  } else {
    STATE.userDoc = null;
  }
}

function openLoginModal() {
  $('#login-modal').hidden = false;
  $('#login-email').focus();
}
function closeLoginModal() {
  $('#login-modal').hidden = true;
  $('#login-error').hidden = true;
  $('#login-form').reset();
}
async function doLogin(email, password) {
  try {
    await signInWithEmailAndPassword(auth, email, password);
    closeLoginModal();
    toast('Bem-vindo!');
  } catch (err) {
    const errEl = $('#login-error');
    errEl.textContent = err.code === 'auth/invalid-credential' ? 'Email ou senha incorretos' : ('Erro: ' + err.message);
    errEl.hidden = false;
  }
}
async function doLogout() {
  await signOut(auth);
  STATE.user = null;
  STATE.userDoc = null;
  clearListeners();
  location.hash = '#/';
  toast('Saiu');
  updateAuthUI();
  renderLoginLanding();
}

function updateAuthUI() {
  const authBtn = $('#btn-auth');
  const nav = $('#site-nav');
  if (STATE.user && STATE.userDoc) {
    authBtn.textContent = 'Sair';
    authBtn.classList.add('logged-in');
    // Show navigation
    nav.classList.add('logged');
    // Show admin-only items
    const navClientes = $('#nav-clientes');
    const navUsuarios = $('#nav-usuarios');
    navClientes && (navClientes.style.display = (isMaster() || isStaff()) ? '' : 'none');
    navUsuarios && (navUsuarios.style.display = canManageUsers() ? '' : 'none');
  } else {
    authBtn.textContent = 'Entrar';
    authBtn.classList.remove('logged-in');
    nav.classList.remove('logged');
  }
}

// ---------- Router ----------
function parseHash() {
  const h = location.hash.slice(1) || '/';
  return h.split('/').filter(Boolean);
}

async function route() {
  const parts = parseHash();
  $$('.site-nav a').forEach(a => a.classList.remove('active'));

  // Not logged in — show login landing
  if (!STATE.user) {
    renderLoginLanding();
    return;
  }
  // Logged in but no user doc (not authorized)
  if (!STATE.userDoc) {
    renderUnauthorized();
    return;
  }

  // Determine where to route
  // Bootstrap route '/'
  if (parts.length === 0) {
    if (isMaster() || isStaff()) { location.hash = '#/clientes'; return; }
    if (isClienteUser()) {
      const cids = STATE.userDoc.clienteIds || [];
      if (cids.length === 0) { renderNoAccess(); return; }
      location.hash = `#/c/${cids[0]}`;
      return;
    }
    renderNoAccess();
    return;
  }

  // Master-only routes
  if (parts[0] === 'admin') {
    if (!isMaster()) { renderNoAccess(); return; }
    if (parts[1] === 'clientes') { await renderClientesAdmin(); return; }
    if (parts[1] === 'usuarios') { await renderUsuariosAdmin(); return; }
  }

  // Clientes list (master + staff)
  if (parts[0] === 'clientes') {
    $('#nav-clientes')?.classList.add('active');
    await renderClientesList();
    return;
  }

  // Cliente-scoped routes: /c/{cid}/...
  if (parts[0] === 'c' && parts[1]) {
    const cid = parts[1];
    if (!canViewCliente(cid)) { renderNoAccess(); return; }
    // Subscribe if not already
    if (STATE.currentClienteId !== cid) {
      subscribeCliente(cid);
      // Render loading while we wait
      renderLoadingScreen();
      return;
    }
    // Wait for loaded
    if (!STATE.loaded) { renderLoadingScreen(); return; }

    const rest = parts.slice(2);
    if (rest.length === 0) { renderClienteHome(cid); return; }
    if (rest[0] === 'ficha' && rest[1]) { renderFicha(cid, rest[1], rest[2] || null); return; }
    if (rest[0] === 'insumos') { renderInsumos(cid); return; }
    if (rest[0] === 'admin') {
      if (!canEditCliente(cid)) { renderNoAccess(); return; }
      if (rest[1] === 'new') { renderAdminEdit(cid, null); return; }
      if (rest[1] === 'edit' && rest[2]) { renderAdminEdit(cid, rest[2]); return; }
      renderAdminList(cid); return;
    }
  }

  // Fallback
  location.hash = '#/';
}
window.addEventListener('hashchange', route);

// ---------- Views: Login / bootstrap / errors ----------
function renderLoginLanding() {
  const app = $('#app');
  app.innerHTML = '';
  const box = el('section', { class: 'login-landing' },
    el('h1', {}, 'Fichas Técnicas'),
    el('p', { class: 'subtitle' }, 'Acesso para consultoria gastronômica'),
    el('button', { class: 'btn btn-primary btn-lg', onclick: openLoginModal }, 'Entrar')
  );
  app.appendChild(box);
}

function renderUnauthorized() {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('section', { class: 'login-landing' },
    el('h1', {}, 'Sem acesso'),
    el('p', { class: 'subtitle' }, 'Sua conta existe mas ainda não foi autorizada por um administrador.'),
    el('button', { class: 'btn', onclick: doLogout }, 'Sair')
  ));
}

function renderNoAccess() {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('section', { class: 'login-landing' },
    el('h1', {}, 'Acesso negado'),
    el('p', { class: 'subtitle' }, 'Você não tem permissão para ver essa página.'),
    el('a', { class: 'btn', href: '#/' }, 'Voltar ao início')
  ));
}

function renderLoadingScreen() {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('div', { class: 'loading-state' },
    el('p', {}, 'Carregando…')
  ));
}

// ---------- Views: Clientes list (master/staff picker) ----------
async function renderClientesList() {
  const app = $('#app');
  app.innerHTML = '';
  renderLoadingScreen();
  await loadClientesList();
  app.innerHTML = '';

  const header = el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Restaurantes'),
      el('p', {}, isMaster() ? 'Todos os restaurantes cadastrados. Clique para entrar.' : 'Restaurantes nos quais você tem acesso.')
    ),
    isMaster() ? el('a', { class: 'btn btn-primary', href: '#/admin/clientes' }, '+ Gerenciar') : null
  );
  app.appendChild(header);

  if (STATE.clientes.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' },
      el('p', {}, isMaster()
        ? 'Nenhum restaurante ainda. Clique em Gerenciar para criar o primeiro.'
        : 'Você ainda não tem acesso a nenhum restaurante. Fale com o administrador.')
    ));
    return;
  }

  const grid = el('div', { class: 'cliente-grid' });
  STATE.clientes.forEach(c => {
    const card = el('a', { class: 'cliente-card', href: `#/c/${c.id}` },
      el('h3', {}, c.name || c.id),
      el('p', { class: 'muted' }, c.slug || c.id)
    );
    grid.appendChild(card);
  });
  app.appendChild(grid);
}

// ---------- Views: Admin Clientes (master only) ----------
async function renderClientesAdmin() {
  const app = $('#app');
  renderLoadingScreen();
  await loadClientesList();
  app.innerHTML = '';

  app.appendChild(el('a', { href: '#/clientes', class: 'back-link' }, '← Voltar para Restaurantes'));
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Gerenciar restaurantes'),
      el('p', {}, 'Crie novos restaurantes, importe dados, sincronize preços ou exclua.')
    )
  ));

  // Create new panel — cleaner form
  const createPanel = el('section', { class: 'admin-panel-card' },
    el('h3', {}, 'Novo restaurante'),
    el('p', { class: 'muted' }, 'Digite o nome do restaurante. O ID é gerado automaticamente.')
  );
  const nameInput = el('input', { type: 'text', placeholder: 'Ex: Diz que me Disse', class: 'inline-input' });
  const createBtn = el('button', { class: 'btn btn-primary', onclick: async () => {
    const name = nameInput.value.trim();
    if (!name) { alert('Informe o nome'); return; }
    const id = slugify(name);
    try {
      await saveCliente({ id, name, slug: id, createdAt: serverTimestamp() });
      toast('Restaurante criado');
      nameInput.value = '';
      renderClientesAdmin();
    } catch (err) { toast('Erro: ' + err.message); }
  } }, '+ Criar');
  createPanel.appendChild(el('div', { class: 'inline-form' }, nameInput, createBtn));
  app.appendChild(createPanel);

  // List existing
  if (STATE.clientes.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' },
      el('p', { class: 'muted' }, 'Nenhum restaurante cadastrado ainda.')
    ));
    return;
  }

  const gridTitle = el('h3', { class: 'section-title' }, `Restaurantes cadastrados (${STATE.clientes.length})`);
  app.appendChild(gridTitle);

  const grid = el('div', { class: 'cliente-admin-grid' });
  STATE.clientes.forEach(c => {
    const card = el('article', { class: 'cliente-admin-card' },
      el('div', { class: 'cc-head' },
        el('h4', { class: 'cc-name' }, c.name),
        el('span', { class: 'cc-id' }, c.id)
      ),
      el('div', { class: 'cc-actions' },
        el('a', { class: 'btn btn-primary btn-block', href: `#/c/${c.id}` }, 'Abrir restaurante →'),
        el('div', { class: 'cc-menu' },
          el('button', { class: 'btn btn-small', onclick: async () => {
            const newName = prompt('Novo nome:', c.name);
            if (!newName || newName === c.name) return;
            try { await saveCliente({ ...c, name: newName }); toast('Renomeado'); renderClientesAdmin(); }
            catch (err) { toast('Erro: ' + err.message); }
          } }, '✎ Renomear'),
          el('button', { class: 'btn btn-small btn-accent', onclick: () => syncPricesOnly(c.id), title: 'Atualiza só os preços dos insumos com valores do data.json' }, '₴ Atualizar preços'),
          el('button', { class: 'btn btn-small', onclick: () => {
            if (!confirm(`Reimportar dados iniciais em "${c.name}"?\n\n• Sobrescreve fichas com versão do data.json\n• Preços existentes > 0 são preservados\n• Insumos e fichas órfãos são excluídos`)) return;
            seedClienteFromJson(c.id, c.name).catch(e => toast('Erro: ' + e.message));
          }, title: 'Reimporta TUDO do data.json (sobrescreve fichas)' }, '⟳ Reimportar tudo'),
          el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
            if (!confirm(`EXCLUIR "${c.name}"?\n\nIsso apaga fichas, insumos e configs. Baixe um backup antes se houver dados importantes.`)) return;
            try {
              const [dSnap, iSnap] = await Promise.all([getDocs(dishesCol(c.id)), getDocs(insumosCol(c.id))]);
              let batch = writeBatch(db), cnt = 0;
              for (const d of dSnap.docs) { batch.delete(dishDoc(c.id, d.id)); if (++cnt >= 400) { await batch.commit(); batch = writeBatch(db); cnt = 0; } }
              for (const d of iSnap.docs) { batch.delete(insumoDoc(c.id, d.id)); if (++cnt >= 400) { await batch.commit(); batch = writeBatch(db); cnt = 0; } }
              batch.delete(configDoc(c.id, 'equipamentos'));
              batch.delete(clienteDoc(c.id));
              await batch.commit();
              toast('Restaurante excluído'); renderClientesAdmin();
            } catch (err) { toast('Erro: ' + err.message); }
          } }, '× Excluir')
        )
      )
    );
    grid.appendChild(card);
  });
  app.appendChild(grid);
}

// ---------- Views: Usuários Admin (master only) ----------
async function renderUsuariosAdmin() {
  const app = $('#app');
  renderLoadingScreen();
  const [usersSnap] = await Promise.all([getDocs(collection(db, 'users'))]);
  await loadClientesList();
  const users = usersSnap.docs.map(d => ({ uid: d.id, ...d.data() }));
  app.innerHTML = '';

  app.appendChild(el('a', { href: '#/clientes', class: 'back-link' }, '← Voltar'));
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Usuários'),
      el('p', {}, 'Equipe e clientes. Papel define o que cada um pode fazer.')
    )
  ));

  // Invite form — cleaner layout
  const invitePanel = el('section', { class: 'admin-panel-card' },
    el('h3', {}, 'Convidar novo usuário'),
    el('p', { class: 'muted' }, 'Ao salvar, um email será enviado com link para a pessoa definir sua própria senha.')
  );

  const emailInput = el('input', { type: 'email', placeholder: 'email@exemplo.com', required: true });
  const nameInput = el('input', { type: 'text', placeholder: 'Nome completo' });
  const roleSelect = el('select', {},
    el('option', { value: 'staff' }, 'Equipe — admin completo nos restaurantes atribuídos'),
    el('option', { value: 'cliente' }, 'Cliente — visualiza e edita só preços'),
    el('option', { value: 'master' }, 'Master — acesso total')
  );
  const clienteChecks = el('div', { class: 'cliente-chip-picker' });
  STATE.clientes.forEach(c => {
    const input = el('input', { type: 'checkbox', value: c.id, id: 'inv-c-' + c.id });
    clienteChecks.appendChild(el('label', { class: 'cliente-chip-option', for: 'inv-c-' + c.id }, input, document.createTextNode(c.name)));
  });
  invitePanel.appendChild(el('div', { class: 'form-grid' },
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Email'), emailInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
    el('label', { class: 'field field-wide' }, el('span', { class: 'label-text' }, 'Papel'), roleSelect)
  ));
  invitePanel.appendChild(el('label', { class: 'field' },
    el('span', { class: 'label-text' }, 'Restaurantes autorizados (obrigatório para staff e cliente)'),
    clienteChecks
  ));
  invitePanel.appendChild(el('div', { class: 'panel-actions' },
    el('button', { class: 'btn btn-primary', onclick: async () => {
      const email = emailInput.value.trim();
      const name = nameInput.value.trim() || email.split('@')[0];
      const role = roleSelect.value;
      const selected = $$('input[type=checkbox]:checked', clienteChecks).map(i => i.value);
      if (!email) { alert('Informe email'); return; }
      if (role !== 'master' && selected.length === 0) { alert('Selecione ao menos um restaurante'); return; }
      try {
        await inviteUser(email, name, role, selected);
      } catch (err) { alert('Erro: ' + err.message); }
    } }, 'Enviar convite')
  ));
  app.appendChild(invitePanel);

  // Existing users — list of cards
  app.appendChild(el('h3', { class: 'section-title' }, `Usuários cadastrados (${users.length})`));
  if (users.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' },
      el('p', { class: 'muted' }, 'Nenhum usuário ainda.')
    ));
    return;
  }
  const grid = el('div', { class: 'user-admin-list' });
  users.forEach(u => grid.appendChild(renderUserCard(u)));
  app.appendChild(grid);
}

function roleLabel(role) {
  return { master: 'Master', staff: 'Equipe', cliente: 'Cliente' }[role] || role || '—';
}

function renderUserCard(u) {
  const isMe = u.uid === STATE.user?.uid;
  const card = el('article', { class: 'user-card' });
  const head = el('div', { class: 'user-card-head' },
    el('div', { class: 'user-id' },
      el('h4', { class: 'user-name' }, u.name || '—'),
      el('span', { class: 'user-email' }, u.email)
    ),
    el('span', { class: 'role-badge role-' + (u.role || 'none') }, roleLabel(u.role))
  );
  card.appendChild(head);

  // Clientes as chips
  if (u.role !== 'master') {
    const chipRow = el('div', { class: 'user-chips' });
    const ids = u.clienteIds || [];
    if (ids.length === 0) {
      chipRow.appendChild(el('span', { class: 'muted' }, 'Nenhum restaurante atribuído'));
    } else {
      ids.forEach(cid => {
        const cname = STATE.clientes.find(c => c.id === cid)?.name || cid;
        chipRow.appendChild(el('span', { class: 'user-chip' }, cname));
      });
    }
    card.appendChild(chipRow);
  } else {
    card.appendChild(el('div', { class: 'user-chips' },
      el('span', { class: 'muted' }, 'Acesso total a todos os restaurantes')));
  }

  // Actions
  const actions = el('div', { class: 'user-actions' });
  if (isMe) {
    actions.appendChild(el('span', { class: 'muted' }, '(você)'));
  } else {
    actions.appendChild(el('button', { class: 'btn btn-small btn-primary', onclick: () => openEditUserModal(u) }, 'Editar'));
    actions.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
      if (!confirm(`Remover acesso de ${u.email}?\n\nIsso só remove permissões no site. A conta no Firebase Auth permanece — delete lá também se quiser banir de vez.`)) return;
      try { await deleteDoc(doc(db, 'users', u.uid)); toast('Removido'); renderUsuariosAdmin(); }
      catch (err) { toast('Erro: ' + err.message); }
    } }, 'Remover'));
  }
  card.appendChild(actions);
  return card;
}

function openEditUserModal(u) {
  // Create modal dynamically
  const existing = $('#edit-user-modal');
  if (existing) existing.remove();

  const emailEl = el('div', { class: 'modal-read' }, el('span', { class: 'label-text' }, 'Email'), el('div', {}, u.email));
  const nameInput = el('input', { type: 'text', value: u.name || '' });
  const roleSelect = el('select', {},
    el('option', { value: 'staff' }, 'Equipe'),
    el('option', { value: 'cliente' }, 'Cliente'),
    el('option', { value: 'master' }, 'Master')
  );
  roleSelect.value = u.role || 'staff';

  const clienteChecks = el('div', { class: 'cliente-chip-picker' });
  const currentIds = new Set(u.clienteIds || []);
  STATE.clientes.forEach(c => {
    const input = el('input', { type: 'checkbox', value: c.id, id: 'edit-c-' + c.id });
    if (currentIds.has(c.id)) input.setAttribute('checked', '');
    clienteChecks.appendChild(el('label', { class: 'cliente-chip-option', for: 'edit-c-' + c.id }, input, document.createTextNode(c.name)));
  });

  const modal = el('div', { class: 'modal', id: 'edit-user-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content modal-content-wide' },
      el('h2', {}, 'Editar usuário'),
      el('p', { class: 'modal-subtitle' }, u.email),
      el('div', { class: 'form-grid' },
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Papel'), roleSelect)
      ),
      el('label', { class: 'field' },
        el('span', { class: 'label-text' }, 'Restaurantes autorizados'),
        clienteChecks
      ),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: async () => {
          const role = roleSelect.value;
          const ids = $$('input[type=checkbox]:checked', clienteChecks).map(i => i.value);
          if (role !== 'master' && ids.length === 0) { alert('Selecione ao menos um restaurante'); return; }
          try {
            await setDoc(doc(db, 'users', u.uid), {
              ...u, name: nameInput.value.trim() || u.email,
              role, clienteIds: ids
            }, { merge: true });
            toast('Atualizado');
            modal.remove();
            renderUsuariosAdmin();
          } catch (err) { toast('Erro: ' + err.message); }
        } }, 'Salvar')
      )
    )
  );
  document.body.appendChild(modal);
}

async function inviteUser(email, name, role, clienteIds) {
  // Create user with a random temp password via secondary Firebase app (doesn't affect current session)
  const tempPw = 'Temp' + Math.random().toString(36).slice(2, 10) + '!1';
  const secondaryApp = initializeApp(firebaseConfig, 'secondary-' + uid());
  const secondaryAuth = getAuth(secondaryApp);
  let newUid = null;
  try {
    const cred = await createUserWithEmailAndPassword(secondaryAuth, email, tempPw);
    newUid = cred.user.uid;
    await signOut(secondaryAuth);
  } catch (err) {
    if (err.code === 'auth/email-already-in-use') {
      // If already exists, we can't know the UID from client-side. Ask admin to input UID or just create user doc with email-indexed hack.
      // For simplicity: use email as doc id fallback (though UID indexing is better)
      alert('Email já possui conta. Crie o doc manualmente ou contate suporte.');
      return;
    }
    throw err;
  }
  // Create user doc
  await setDoc(doc(db, 'users', newUid), {
    email, name, role, clienteIds, createdAt: serverTimestamp()
  });
  // Send password reset email
  try {
    await sendPasswordResetEmail(auth, email);
  } catch (err) { console.warn('reset email failed', err); }
  toast('Usuário criado. Email de definição de senha enviado.');
  renderUsuariosAdmin();
}

// ---------- Cost calculations (same core as before) ----------
function findInsumo(id) { return STATE.insumos.find(i => i.id === id); }
function nrm(s) {
  if (!s) return '';
  return String(s).trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\(.*?\)/g, '').replace(/\s+/g, ' ').trim().replace(/[,.;:\s]+$/, '');
}
function detectSubref(dish, currentSfIdx, ingredientName) {
  const ingClean = nrm(ingredientName);
  if (ingClean.length < 4) return null;
  let best = null, bestScore = 0;
  for (let j = 0; j < currentSfIdx; j++) {
    const other = dish.sub_fichas[j];
    const otherN = nrm(other.name);
    if (!otherN) continue;
    if (otherN === ingClean) return other;
    if (otherN.includes(ingClean) || ingClean.includes(otherN)) {
      const ratio = Math.min(otherN.length, ingClean.length) / Math.max(otherN.length, ingClean.length);
      if (ratio > bestScore) { bestScore = ratio; best = other; }
    }
  }
  return bestScore > 0.55 ? best : null;
}
function parseRendimentoQty(rendStr) {
  if (!rendStr) return { qty: 1, unit: '' };
  const s = String(rendStr).trim();
  const m = s.match(/^(\d+(?:[.,]\d+)?)\s*([a-zA-Zµ\/\u00c0-\u024f]*)/);
  if (m) return { qty: parseFloat(m[1].replace(',', '.')), unit: (m[2] || '').toLowerCase() };
  return { qty: 1, unit: '' };
}
function subfichaCost(sf, dish, cache = null, visited = null) {
  cache = cache || new Map();
  visited = visited || new Set();
  if (cache.has(sf.id)) return cache.get(sf.id);
  if (visited.has(sf.id)) return { rows: [], total: 0 };
  visited.add(sf.id);
  const sfIdx = dish.sub_fichas.findIndex(s => s.id === sf.id);
  let total = 0;
  const rows = (sf.ingredientes || []).map(ing => {
    let subSf = null;
    if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
    if (!subSf && sfIdx > 0) subSf = detectSubref(dish, sfIdx, ing.insumo_name);
    if (subSf) {
      const subResult = subfichaCost(subSf, dish, cache, new Set(visited));
      const subRend = parseRendimentoQty(subSf.rendimento);
      const ingQty = ing.qty;
      let cost = 0;
      if (ingQty != null && subRend.qty > 0) {
        const [qConverted] = normalizeSubrefQty(ingQty, ing.unit, subRend.unit);
        cost = (qConverted / subRend.qty) * subResult.total;
      }
      total += cost;
      return { ing, insumo: null, cost, subSf, isSubref: true };
    }
    const insumo = findInsumo(ing.insumo_id);
    const [qNorm, priceNorm] = normalizeQtyPrice(ing, insumo);
    const fc = (typeof ing.fc === 'number' && ing.fc > 0) ? ing.fc : 1;
    const cost = (qNorm != null && priceNorm != null) ? qNorm * priceNorm * fc : 0;
    total += cost;
    return { ing, insumo, cost, isSubref: false, fc };
  });
  const result = { rows, total };
  cache.set(sf.id, result);
  return result;
}
function normalizeSubrefQty(qty, ingUnit, subUnit) {
  const iu = (ingUnit || '').toLowerCase().trim();
  const su = (subUnit || '').toLowerCase().trim();
  if (iu === su || !iu || !su) return [qty, qty];
  if (iu === 'g' && su === 'kg') return [qty / 1000, qty];
  if (iu === 'kg' && su === 'g') return [qty * 1000, qty];
  if (iu === 'ml' && (su === 'l' || su === 'lt' || su === 'litro')) return [qty / 1000, qty];
  if (iu === 'l' && su === 'ml') return [qty * 1000, qty];
  return [qty, qty];
}
function normalizeQtyPrice(ing, insumo) {
  if (!insumo || ing.qty == null) return [null, null];
  const iu = (ing.unit || '').toLowerCase().trim();
  const pu = (insumo.unit || '').toLowerCase().trim();
  const q = ing.qty;
  const p = insumo.price || 0;
  if (iu === pu || !iu || !pu) return [q, p];
  if (iu === 'g' && pu === 'kg') return [q / 1000, p];
  if (iu === 'kg' && pu === 'g') return [q * 1000, p];
  if (iu === 'ml' && (pu === 'l' || pu === 'lt' || pu === 'litro')) return [q / 1000, p];
  if (iu === 'l' && pu === 'ml') return [q * 1000, p];
  return [q, p];
}
function dishCost(dish) {
  const cache = new Map();
  const sfCosts = (dish.sub_fichas || []).map(sf => {
    const r = subfichaCost(sf, dish, cache);
    return { ...r, sf };
  });
  const finalSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
  const finalResult = finalSf ? sfCosts.find(x => x.sf.id === finalSf.id) : null;
  const total = finalResult ? finalResult.total : 0;
  const portions = parsePortions(finalSf ? finalSf.rendimento : '');
  const costPerPortion = portions > 0 ? total / portions : 0;
  // Preço sugerido: se dish.target_cmv definido, usa CMV alvo; senão usa markup legado
  let suggestedPrice, cmv, markup;
  if (typeof dish.target_cmv === 'number' && dish.target_cmv > 0) {
    cmv = dish.target_cmv;
    suggestedPrice = costPerPortion / (cmv / 100);
    markup = costPerPortion > 0 ? ((suggestedPrice / costPerPortion) - 1) * 100 : 0;
  } else {
    markup = dish.markup || 300;
    suggestedPrice = costPerPortion * (1 + markup / 100);
    cmv = suggestedPrice > 0 ? (costPerPortion / suggestedPrice) * 100 : 0;
  }
  return { sfCosts, total, portions, costPerPortion, suggestedPrice, cmv, markup };
}
function parsePortions(rendStr) {
  if (!rendStr) return 1;
  const s = String(rendStr).toLowerCase();
  const m = s.match(/(\d+(?:[.,]\d+)?)\s*(?:und|unidades?|por[cç][oõ]es?|por[cç][aã]o)/);
  if (m) return parseFloat(m[1].replace(',', '.'));
  const m2 = s.match(/^(\d+(?:[.,]\d+)?)/);
  if (m2) return parseFloat(m2[1].replace(',', '.'));
  return 1;
}

// Scale an ingredient quantity by a factor
function scaleQty(qty, factor) {
  if (qty == null) return qty;
  return qty * factor;
}
function scaleRendStr(rendStr, factor) {
  // Multiply the leading number in rendimento string
  if (!rendStr) return rendStr;
  return String(rendStr).replace(/^(\d+(?:[.,]\d+)?)/, (m) => {
    const n = parseFloat(m.replace(',', '.')) * factor;
    // Round to at most 2 decimals
    const rounded = Math.round(n * 100) / 100;
    return String(rounded).replace('.', ',');
  });
}

// ---------- Cascade scale: target quantity at FINAL sub-ficha propagates up the chain ----------
function computeCascadeScales(dish, finalTargetQty) {
  const scales = {};
  const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
  if (!finalSf) return scales;
  const finalRend = parseRendimentoQty(finalSf.rendimento);
  const finalScale = finalRend.qty > 0 ? finalTargetQty / finalRend.qty : 1;
  scales[finalSf.id] = finalScale;

  function propagate(sfId, scale, visited = new Set()) {
    if (visited.has(sfId)) return;
    visited.add(sfId);
    const sf = dish.sub_fichas.find(s => s.id === sfId);
    if (!sf) return;
    for (const ing of sf.ingredientes || []) {
      if (ing.subref_id) {
        const ref = dish.sub_fichas.find(s => s.id === ing.subref_id);
        if (!ref) continue;
        const refRend = parseRendimentoQty(ref.rendimento);
        if (refRend.qty <= 0 || ing.qty == null) continue;
        // Normalize units (ing.unit and refRend.unit)
        const [qC] = normalizeSubrefQty(ing.qty, ing.unit, refRend.unit);
        const needed = qC * scale;
        const refScale = needed / refRend.qty;
        // Accumulate (multiple sub-fichas could reference the same)
        scales[ref.id] = (scales[ref.id] || 0) + refScale;
        propagate(ref.id, refScale, visited);
      }
    }
  }
  propagate(finalSf.id, finalScale);
  return scales;
}
// ---------- Views: Cliente home (cardápio) — list format ----------
function renderClienteHome(cid) {
  const app = $('#app');
  app.innerHTML = '';
  const totalSubfichas = STATE.dishes.reduce((s, d) => s + (d.sub_fichas || []).length, 0);

  app.appendChild(renderClienteContext(cid));

  const hero = el('section', { class: 'home-hero' },
    el('h1', {}, 'Cardápio'),
    el('p', { class: 'subtitle' }, STATE.currentCliente?.name || ''),
    el('div', { class: 'home-stats' },
      el('span', {}, el('strong', {}, STATE.dishes.length.toString()), 'Pratos'),
      el('span', {}, el('strong', {}, totalSubfichas.toString()), 'Sub-fichas'),
      el('span', {}, el('strong', {}, STATE.insumos.length.toString()), 'Insumos')
    )
  );
  app.appendChild(hero);

  if (STATE.dishes.length === 0) {
    const empty = el('div', { class: 'empty-state' },
      el('p', {}, 'Esse cliente ainda não tem fichas cadastradas.'),
      canEditCliente(cid) ? el('button', { class: 'btn btn-primary', onclick: () => {
        if (confirm(`Importar dados iniciais (da planilha original) em "${STATE.currentCliente?.name || cid}"?`))
          seedClienteFromJson(cid, STATE.currentCliente?.name).catch(e => toast('Erro: ' + e.message));
      } }, 'Importar dados iniciais') : null,
      canEditCliente(cid) ? el('a', { class: 'btn', href: `#/c/${cid}/admin/new` }, '+ Criar primeira ficha') : null,
    );
    app.appendChild(empty);
    return;
  }

  const listWrap = el('div', { class: 'cardapio-list' });
  STATE.dishes.forEach((dish, idx) => {
    const { costPerPortion } = dishCost(dish);
    const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
    const rendDisplay = lastSf?.rendimento ? formatRendimento(lastSf.rendimento) : '—';
    const dishGroup = el('article', { class: 'dish-group' },
      el('header', { class: 'dish-head' },
        el('a', { class: 'dish-title', href: `#/c/${cid}/ficha/${dish.id}` },
          el('span', { class: 'dish-number' }, String(idx + 1).padStart(2, '0')),
          el('span', { class: 'dish-name' }, dish.name)
        ),
        el('div', { class: 'dish-meta' },
          el('span', { class: 'meta-item' }, el('em', {}, 'rendimento: '), rendDisplay),
          canEditInsumoPrice(cid) ? el('span', { class: 'meta-item' }, el('em', {}, 'custo/porção: '), fmtBRL(costPerPortion)) : null
        )
      ),
      (() => {
        const ul = el('ul', { class: 'subficha-sublist' });
        (dish.sub_fichas || []).forEach((sf, sfIdx) => {
          const isFinal = sfIdx === dish.sub_fichas.length - 1;
          ul.appendChild(el('li', { class: 'subficha-item' + (isFinal ? ' is-final' : '') },
            el('a', { href: `#/c/${cid}/ficha/${dish.id}/${sf.id}` },
              el('span', { class: 'sf-marker' }, isFinal ? '●' : '○'),
              el('span', { class: 'sf-name' }, sf.name),
              sf.rendimento ? el('span', { class: 'sf-rend' }, formatRendimento(sf.rendimento)) : null
            )
          ));
        });
        return ul;
      })()
    );
    listWrap.appendChild(dishGroup);
  });
  app.appendChild(listWrap);
}

function renderClienteContext(cid) {
  const bar = el('div', { class: 'context-bar' });
  if (isMaster() || isStaff()) {
    bar.appendChild(el('a', { href: '#/clientes', class: 'back-link' }, '← Todos os restaurantes'));
  }
  bar.appendChild(el('div', { class: 'ctx-links' },
    el('a', { class: 'ctx-link', href: `#/c/${cid}` }, 'Cardápio'),
    el('a', { class: 'ctx-link', href: `#/c/${cid}/insumos` }, 'Insumos'),
    canEditCliente(cid) ? el('a', { class: 'ctx-link', href: `#/c/${cid}/admin` }, 'Gerenciar') : null,
  ));
  return bar;
}

// ---------- Views: Ficha (with scaling + kitchen mode on mobile) ----------
function renderFicha(cid, dishId, initialSfId = null) {
  const dish = STATE.dishes.find(d => d.id === dishId);
  const app = $('#app');
  app.innerHTML = '';
  if (!dish) {
    app.appendChild(el('p', {}, 'Prato não encontrado. ', el('a', { href: `#/c/${cid}` }, 'Voltar')));
    return;
  }
  app.appendChild(renderClienteContext(cid));

  const defaultSf = initialSfId && dish.sub_fichas.some(s => s.id === initialSfId)
    ? initialSfId
    : (dish.sub_fichas[0]?.id || null);
  // Escala em cascata: define rendimento alvo APENAS no prato final; propaga pra todas as sub-fichas.
  const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
  const originalFinal = parseRendimentoQty(finalSf?.rendimento);
  const state = {
    view: 'trabalho',
    activeSf: defaultSf,
    finalTargetQty: originalFinal.qty,  // padrão = rendimento original
    finalUnit: originalFinal.unit,
  };
  state.scales = () => computeCascadeScales(dish, state.finalTargetQty);

  const header = el('div', { class: 'ficha-header' },
    el('a', { class: 'back-link', href: `#/c/${cid}` }, '← Voltar ao cardápio'),
    el('h1', {}, dish.name)
  );
  app.appendChild(header);

  if (dish.photos && dish.photos.length) {
    const gallery = el('div', { class: 'gallery' });
    dish.photos.forEach(p => gallery.appendChild(el('div', { class: 'photo' }, el('img', { src: p }))));
    app.appendChild(gallery);
  }

  const toggle = el('div', { class: 'view-toggle' },
    el('button', { 'data-view': 'trabalho' }, 'Ficha de trabalho'),
    el('button', { 'data-view': 'custo' }, 'Ficha de custo')
  );
  app.appendChild(toggle);

  // Production scale bar (dish-level) — aparece só na ficha de trabalho
  const scaleBar = el('div', { class: 'rend-bar dish-scale-bar' },
    el('div', { class: 'rend-info' },
      el('span', { class: 'rend-label' }, 'Rendimento original'),
      el('span', { class: 'rend-value' }, formatRendimento(finalSf?.rendimento || '—'))
    ),
    el('div', { class: 'scale-ctrl' },
      el('span', { class: 'scale-label' }, 'Produzir:'),
      (() => {
        const input = el('input', { type: 'number', step: '1', min: '0.001',
          value: state.finalTargetQty });
        input.addEventListener('input', () => {
          const v = parseFloat(input.value.replace(',', '.'));
          if (isNaN(v) || v <= 0) return;
          state.finalTargetQty = v;
          updateUI();
        });
        return input;
      })(),
      el('span', { class: 'scale-unit' }, state.finalUnit || ''),
      (() => {
        const btn = el('button', { class: 'btn btn-small btn-ghost', onclick: () => {
          state.finalTargetQty = originalFinal.qty;
          updateUI();
        } }, 'Resetar');
        btn.style.display = 'none';
        btn.id = 'scale-reset';
        return btn;
      })()
    )
  );
  app.appendChild(scaleBar);

  const tabs = el('div', { class: 'subficha-tabs' });
  (dish.sub_fichas || []).forEach(sf => {
    tabs.appendChild(el('button', { 'data-sf': sf.id }, sf.name));
  });
  app.appendChild(tabs);

  const body = el('div', {});
  app.appendChild(body);

  const actions = el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn', onclick: () => exportFichaPDF(dish, state) }, 'Exportar PDF'),
    el('button', { class: 'btn', onclick: () => exportFichaXLSX(dish, state) }, 'Exportar Excel')
  );
  app.appendChild(actions);

  function updateUI() {
    $$('.view-toggle button', toggle).forEach(b => b.classList.toggle('active', b.dataset.view === state.view));
    $$('.subficha-tabs button', tabs).forEach(b => b.classList.toggle('active', b.dataset.sf === state.activeSf));
    // Scale bar visible só no modo trabalho
    scaleBar.style.display = state.view === 'trabalho' ? '' : 'none';
    const resetBtn = $('#scale-reset', scaleBar);
    if (resetBtn) resetBtn.style.display = (state.finalTargetQty !== originalFinal.qty) ? '' : 'none';
    const sf = (dish.sub_fichas || []).find(s => s.id === state.activeSf) || (dish.sub_fichas || [])[0];
    body.innerHTML = '';
    if (state.view === 'trabalho') body.appendChild(renderFichaTrabalho(dish, sf, state));
    else body.appendChild(renderFichaCusto(dish, sf, cid));
  }
  toggle.addEventListener('click', e => { if (e.target.dataset.view) { state.view = e.target.dataset.view; updateUI(); } });
  tabs.addEventListener('click', e => { if (e.target.dataset.sf) { state.activeSf = e.target.dataset.sf; updateUI(); } });
  updateUI();
}

function renderFichaTrabalho(dish, sf, state) {
  const wrap = el('div', { class: 'ficha-body kitchen-mode' });
  wrap.appendChild(el('div', { class: 'ficha-meta-line' }, 'Ficha Técnica Operacional'));
  wrap.appendChild(el('h2', {}, sf.name));

  // Escala em cascata: pega a escala desta sub-ficha baseada no alvo final
  const scales = state.scales();
  const sfScale = scales[sf.id] || 1;

  // Mostra rendimento desta sub-ficha (escalado)
  const parsedRend = parseRendimentoQty(sf.rendimento || '');
  const scaledRendQty = parsedRend.qty * sfScale;
  const normRend = normUnitForDisplay(scaledRendQty, parsedRend.unit);
  const rendInfo = el('div', { class: 'ficha-meta-line' },
    'Rendimento: ',
    el('strong', {}, `${normRend.text} ${normRend.unit}`.trim()),
    sfScale !== 1 ? el('span', { class: 'muted', style: 'margin-left:0.5rem;' }, `(${fmtNum(sfScale, 2)}× do original)`) : null
  );
  wrap.appendChild(rendInfo);

  // Ingredients (kitchen style) — quantities scaled by this sub-ficha's cascade factor
  const sfIdx = dish.sub_fichas.findIndex(s => s.id === sf.id);
  const ingList = el('ul', { class: 'kitchen-ingredients' });
  (sf.ingredientes || []).forEach(ing => {
    let subSf = null;
    if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
    if (!subSf && sfIdx > 0) subSf = detectSubref(dish, sfIdx, ing.insumo_name);

    const formatted = formatIngQty(ing, sfScale);
    const li = el('li', { class: 'kitchen-ing' + (subSf ? ' is-subref' : '') },
      el('div', { class: 'k-row-main' },
        el('span', { class: 'k-name' }, (subSf ? '↪ ' : '') + ing.insumo_name),
        el('span', { class: 'k-qty' },
          el('span', { class: 'qty-num' }, formatted.text),
          formatted.unit ? el('span', { class: 'qty-unit' }, formatted.unit) : null
        )
      ),
      ing.observacao ? el('div', { class: 'k-obs' }, ing.observacao) : null,
      subSf ? el('div', { class: 'k-sub-note' }, 'ver preparação: ' + subSf.name) : null
    );
    ingList.appendChild(li);
  });
  wrap.appendChild(el('h3', { class: 'k-section-title' }, 'Ingredientes'));
  wrap.appendChild(ingList);

  if (sf.modo_preparo) {
    wrap.appendChild(el('h3', { class: 'k-section-title' }, 'Modo de preparo'));
    wrap.appendChild(el('div', { class: 'kitchen-preparo' }, sf.modo_preparo));
  }
  if (dish.louca) {
    wrap.appendChild(el('div', { class: 'info-box' },
      el('strong', {}, 'Apresentação / Louça'),
      dish.louca
    ));
  }
  if (dish.equipamentos && dish.equipamentos.length) {
    const box = el('div', { class: 'info-box' }, el('strong', {}, 'Equipamentos necessários'));
    const list = el('div', { class: 'equipamentos-list' });
    dish.equipamentos.forEach(eq => list.appendChild(el('span', { class: 'chip' }, eq)));
    box.appendChild(list);
    wrap.appendChild(box);
  }

  return wrap;
}

function renderFichaCusto(dish, currentSf, cid) {
  const wrap = el('div', { class: 'ficha-body' });
  if (!canEditInsumoPrice(cid)) {
    wrap.appendChild(el('p', { class: 'muted' }, 'Ficha de custo não disponível para seu perfil.'));
    return wrap;
  }
  wrap.appendChild(el('div', { class: 'ficha-meta-line' }, 'Ficha de Custo por Rendimento'));
  wrap.appendChild(el('h2', {}, dish.name));

  const all = dishCost(dish);
  all.sfCosts.forEach(({ sf, rows, total }) => {
    const rendDisplay = sf.rendimento ? formatRendimento(sf.rendimento) : '';
    const section = el('div', { class: 'ficha-section' },
      el('h3', {}, sf.name + (rendDisplay ? ` — rendimento: ${rendDisplay}` : ''))
    );
    const tbl = el('table', { class: 'ficha-table' },
      el('thead', {}, el('tr', {},
        el('th', {}, 'Insumo'),
        el('th', { class: 'num' }, 'Quantidade'),
        el('th', {}, 'Unidade'),
        el('th', { class: 'num' }, 'Preço unit.'),
        el('th', { class: 'num' }, 'Custo')
      )),
      el('tbody', {}, ...rows.map(({ ing, insumo, cost, isSubref, subSf, fc }) => {
        const fmt = formatIngQty(ing);
        let priceTxt, nameCell;
        if (isSubref && subSf) {
          priceTxt = el('span', { class: 'subref-note' }, `sub-ficha (${formatRendimento(subSf.rendimento || '')})`);
          nameCell = el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↪ '), ing.insumo_name);
        } else {
          const normPrice = insumo ? normalizePriceForDisplay(insumo) : null;
          priceTxt = normPrice ? `${fmtBRL(normPrice.price)} / ${normPrice.unit}` : '—';
          // Show FC badge next to name if applicable
          const fcBadge = (fc && fc > 1) ? el('span', { class: 'fc-badge', title: `Fator de correção ${fc}x` }, ` · FC ${fmtNum(fc, 2)}`) : null;
          nameCell = el('td', { 'data-label': 'Insumo' }, ing.insumo_name, fcBadge);
        }
        return el('tr', { class: isSubref ? 'row-subref' : '' },
          nameCell,
          el('td', { class: 'num', 'data-label': 'Quantidade' }, fmt.text),
          el('td', { 'data-label': 'Unidade' }, fmt.unit || '—'),
          el('td', { class: 'num', 'data-label': 'Preço unit.' }, priceTxt),
          el('td', { class: 'num', 'data-label': 'Custo' }, fmtBRL(cost))
        );
      })),
      el('tfoot', {}, el('tr', {},
        el('td', { colspan: '4' }, 'Subtotal — ' + sf.name),
        el('td', { class: 'num' }, fmtBRL(total))
      ))
    );
    section.appendChild(tbl);
    wrap.appendChild(section);
  });

  const total = el('div', { class: 'total-dish-cost' },
    el('div', {}, el('span', { class: 'stat-label' }, 'Custo total do prato'),
      el('span', { class: 'stat-value' }, fmtBRL(all.total))),
    el('div', {}, el('span', { class: 'stat-label' }, 'Porções (aprox.)'),
      el('span', { class: 'stat-value' }, fmtNum(all.portions, 0))),
    el('div', {}, el('span', { class: 'stat-label' }, 'Custo por porção'),
      el('span', { class: 'stat-value gold' }, fmtBRL(all.costPerPortion)))
  );
  wrap.appendChild(total);

  // 3 campos bidirecionais: CMV / Markup / Preço de Venda
  // Edite qualquer um → os outros 2 se ajustam. Armazena target_cmv como fonte de verdade.
  const costBox = el('div', { class: 'cost-summary' });
  const costPP = all.costPerPortion;
  const initialCmv = dish.target_cmv || 30;
  const initialPrice = costPP > 0 ? costPP / (initialCmv / 100) : 0;
  const initialMarkup = costPP > 0 ? ((initialPrice / costPP) - 1) * 100 : 0;

  const editable = canEditCliente(cid);
  const cmvInput = el('input', { type: 'number', min: '1', max: '100', step: '0.5', value: initialCmv.toFixed(1) });
  const markupInput = el('input', { type: 'number', min: '0', step: '1', value: initialMarkup.toFixed(0) });
  const priceInput = el('input', { type: 'number', min: '0', step: '0.50', value: initialPrice.toFixed(2) });
  if (!editable) { cmvInput.disabled = true; markupInput.disabled = true; priceInput.disabled = true; }

  costBox.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'CMV (%)'), cmvInput));
  costBox.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'Markup (%)'), markupInput));
  costBox.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'Preço de venda'), priceInput));
  wrap.appendChild(costBox);

  // Handlers — evita loop infinito usando flag
  let internalUpdate = false;
  function save() { scheduleSave('dish-' + dish.id, () => saveDish(STATE.currentClienteId, dish)); }

  cmvInput.addEventListener('input', () => {
    if (internalUpdate) return;
    const cmv = parseFloat(cmvInput.value);
    if (isNaN(cmv) || cmv <= 0) return;
    dish.target_cmv = cmv;
    const price = costPP / (cmv / 100);
    const markup = ((price / costPP) - 1) * 100;
    internalUpdate = true;
    priceInput.value = price.toFixed(2);
    markupInput.value = markup.toFixed(0);
    internalUpdate = false;
    save();
  });
  markupInput.addEventListener('input', () => {
    if (internalUpdate) return;
    const markup = parseFloat(markupInput.value);
    if (isNaN(markup) || markup < 0) return;
    const price = costPP * (1 + markup / 100);
    const cmv = price > 0 ? (costPP / price) * 100 : 0;
    dish.target_cmv = cmv;
    internalUpdate = true;
    cmvInput.value = cmv.toFixed(1);
    priceInput.value = price.toFixed(2);
    internalUpdate = false;
    save();
  });
  priceInput.addEventListener('input', () => {
    if (internalUpdate) return;
    const price = parseFloat(priceInput.value);
    if (isNaN(price) || price <= 0) return;
    const cmv = (costPP / price) * 100;
    const markup = ((price / costPP) - 1) * 100;
    dish.target_cmv = cmv;
    internalUpdate = true;
    cmvInput.value = cmv.toFixed(1);
    markupInput.value = markup.toFixed(0);
    internalUpdate = false;
    save();
  });

  return wrap;
}

// ---------- Views: Insumos ----------
function renderInsumos(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  const canEdit = canEditInsumoPrice(cid);
  const header = el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Insumos'),
      el('p', {}, canEdit
        ? 'Atualize os preços — salva automaticamente e todas as fichas recalculam.'
        : 'Preços dos insumos.')
    ),
    el('input', { type: 'search', class: 'insumos-search', placeholder: 'Buscar insumo…' })
  );
  app.appendChild(header);

  const tbl = el('table', { class: 'insumos-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', {}, 'Unidade'),
      el('th', {}, 'Preço (R$)'),
      el('th', {}, 'Usado em'),
      canEditCliente(cid) ? el('th', {}, '') : null
    ))
  );
  const tbody = el('tbody', {});
  const usageMap = {};
  STATE.dishes.forEach(d => (d.sub_fichas || []).forEach(sf =>
    (sf.ingredientes || []).forEach(i => { usageMap[i.insumo_id] = (usageMap[i.insumo_id] || 0) + 1; })
  ));
  function buildRows(filter) {
    tbody.innerHTML = '';
    const f = (filter || '').toLowerCase().trim();
    STATE.insumos.filter(i => !f || i.name.toLowerCase().includes(f)).forEach(insumo => {
      const unitInput = el('input', { class: 'unit-input', value: insumo.unit || '', placeholder: 'g' });
      const priceInput = el('input', { type: 'number', min: '0', step: '0.01', value: insumo.price || 0 });
      if (!canEdit) { unitInput.disabled = true; priceInput.disabled = true; }
      unitInput.addEventListener('change', () => {
        insumo.unit = unitInput.value.trim();
        scheduleSave('ins-' + insumo.id, () => saveInsumo(cid, insumo));
      });
      priceInput.addEventListener('input', () => {
        insumo.price = parseFloat(priceInput.value) || 0;
        scheduleSave('ins-' + insumo.id, () => saveInsumo(cid, insumo));
      });
      const cells = [
        el('td', { 'data-label': 'Insumo' }, insumo.name),
        el('td', { 'data-label': 'Unidade' }, unitInput),
        el('td', { 'data-label': 'Preço (R$)' }, priceInput),
        el('td', { 'data-label': 'Usado em' }, (usageMap[insumo.id] || 0) + ' receitas'),
      ];
      if (canEditCliente(cid)) {
        cells.push(el('td', { 'data-label': '' },
          el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
            const uses = usageMap[insumo.id] || 0;
            if (!confirm(uses > 0 ? `Excluir "${insumo.name}"? Usado em ${uses} receita(s).` : `Excluir "${insumo.name}"?`)) return;
            try { await deleteInsumo(cid, insumo.id); toast('Excluído'); } catch (e) { toast('Erro: ' + e.message); }
          } }, 'Excluir')
        ));
      }
      tbody.appendChild(el('tr', {}, ...cells));
    });
  }
  buildRows('');
  tbl.appendChild(tbody);
  const tableWrap = el('div', { class: 'insumos-table-wrap' }, tbl);
  app.appendChild(tableWrap);
  $('.insumos-search', header).addEventListener('input', e => buildRows(e.target.value));
}

// ---------- Views: Admin dishes ----------
function renderAdminList(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  const header = el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Gerenciar fichas'),
      el('p', {}, 'Adicione, edite ou exclua pratos. Salva automaticamente.')
    ),
    el('a', { class: 'btn btn-primary', href: `#/c/${cid}/admin/new` }, '+ Nova ficha')
  );
  app.appendChild(header);
  const panel = el('div', { class: 'admin-panel' });
  const list = el('div', { class: 'dish-admin-list' });
  STATE.dishes.forEach(dish => {
    const item = el('div', { class: 'dish-admin-item' },
      el('div', { class: 'info' },
        el('h4', {}, dish.name),
        el('p', {}, `${(dish.sub_fichas || []).length} sub-fichas · ${dish.photos?.length || 0} fotos`)
      ),
      el('div', { class: 'dish-admin-actions' },
        el('a', { class: 'btn btn-small', href: `#/c/${cid}/ficha/${dish.id}` }, 'Ver'),
        el('a', { class: 'btn btn-small btn-primary', href: `#/c/${cid}/admin/edit/${dish.id}` }, 'Editar'),
        el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
          if (!confirm(`Excluir "${dish.name}"?`)) return;
          try { await deleteDish(cid, dish.id); toast('Excluída'); } catch (e) { toast('Erro: ' + e.message); }
        } }, 'Excluir')
      )
    );
    list.appendChild(item);
  });
  panel.appendChild(list);
  app.appendChild(panel);
}

function renderAdminEdit(cid, dishId) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  let dish;
  if (dishId) {
    const existing = STATE.dishes.find(d => d.id === dishId);
    if (!existing) { app.appendChild(el('p', {}, 'Prato não encontrado')); return; }
    dish = JSON.parse(JSON.stringify(existing));
  } else {
    dish = { id: 'new-' + uid(), name: '', description: '', photos: [], louca: '', equipamentos: [],
      sub_fichas: [{ id: uid(), name: 'Preparação 1', rendimento: '', ingredientes: [], modo_preparo: '' }],
      markup: 300 };
  }
  app.appendChild(el('a', { href: `#/c/${cid}/admin`, class: 'back-link' }, '← Voltar'));
  app.appendChild(el('h1', {}, dishId ? 'Editar ficha' : 'Nova ficha'));
  const panel = el('div', { class: 'admin-panel' });
  panel.appendChild(el('h2', {}, 'Informações gerais'));
  panel.appendChild(el('div', { class: 'form-grid' },
    fieldInput('Nome do prato', 'text', dish.name, v => dish.name = v),
    fieldInput('Louça / apresentação', 'text', dish.louca, v => dish.louca = v),
    fieldInput('CMV alvo (%)', 'number', dish.target_cmv || 30, v => dish.target_cmv = parseFloat(v) || 30)
  ));
  panel.appendChild(el('h3', {}, 'Fotos (até 3)'));
  const photoWrap = el('div', { class: 'photo-upload' });
  function renderPhotos() {
    photoWrap.innerHTML = '';
    for (let i = 0; i < 3; i++) {
      const photo = dish.photos[i];
      const slot = el('label', { class: 'photo-slot' });
      if (photo) {
        slot.appendChild(el('img', { src: photo }));
        slot.appendChild(el('button', { class: 'remove', onclick: e => { e.preventDefault(); dish.photos.splice(i, 1); renderPhotos(); } }, '×'));
      } else {
        slot.appendChild(document.createTextNode('+ foto'));
        const input = el('input', { type: 'file', accept: 'image/*', style: 'display:none' });
        input.addEventListener('change', () => {
          const file = input.files[0]; if (!file) return;
          const reader = new FileReader();
          reader.onload = () => {
            const img = new Image();
            img.onload = () => {
              const maxW = 1000;
              const scale = Math.min(1, maxW / img.width);
              const canvas = document.createElement('canvas');
              canvas.width = img.width * scale; canvas.height = img.height * scale;
              canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
              dish.photos[i] = canvas.toDataURL('image/jpeg', 0.78);
              renderPhotos();
            };
            img.src = reader.result;
          };
          reader.readAsDataURL(file);
        });
        slot.appendChild(input);
      }
      photoWrap.appendChild(slot);
    }
  }
  renderPhotos();
  panel.appendChild(photoWrap);

  panel.appendChild(el('h3', {}, 'Equipamentos necessários'));
  const eqBox = el('div', { class: 'equipamentos-select' });
  (STATE.equipamentos || []).forEach(eq => {
    const i = el('input', { type: 'checkbox' });
    if (dish.equipamentos.includes(eq)) i.setAttribute('checked', '');
    i.addEventListener('change', () => {
      if (i.checked) { if (!dish.equipamentos.includes(eq)) dish.equipamentos.push(eq); }
      else dish.equipamentos = dish.equipamentos.filter(x => x !== eq);
    });
    eqBox.appendChild(el('label', {}, i, document.createTextNode(' ' + eq)));
  });
  panel.appendChild(eqBox);

  panel.appendChild(el('h2', {}, 'Sub-fichas (preparações)'));
  const sfWrap = el('div', {});
  function renderSubfichas() {
    sfWrap.innerHTML = '';
    dish.sub_fichas.forEach((sf, idx) => {
      const box = el('div', { class: 'subficha-editor' },
        el('h4', {}, el('span', {}, `${idx + 1}. Preparação`),
          el('span', {},
            el('button', { class: 'btn btn-small', onclick: () => { if (idx > 0) { [dish.sub_fichas[idx-1], dish.sub_fichas[idx]] = [dish.sub_fichas[idx], dish.sub_fichas[idx-1]]; renderSubfichas(); } } }, '↑'), ' ',
            el('button', { class: 'btn btn-small', onclick: () => { if (idx < dish.sub_fichas.length - 1) { [dish.sub_fichas[idx], dish.sub_fichas[idx+1]] = [dish.sub_fichas[idx+1], dish.sub_fichas[idx]]; renderSubfichas(); } } }, '↓'), ' ',
            el('button', { class: 'btn btn-small btn-danger', onclick: () => { if (confirm('Excluir esta preparação?')) { dish.sub_fichas.splice(idx, 1); renderSubfichas(); } } }, 'Excluir')
          )
        ),
        el('div', { class: 'form-grid' },
          fieldInput('Nome da preparação', 'text', sf.name, v => sf.name = v),
          fieldInput('Rendimento', 'text', sf.rendimento, v => sf.rendimento = v)
        )
      );
      box.appendChild(el('h4', {}, 'Insumos'));
      const ingList = el('div', {});
      function renderIngs() {
        ingList.innerHTML = '';
        sf.ingredientes.forEach((ing, ingIdx) => {
          const row = el('div', { class: 'ingredient-row' });
          const nameInput = el('input', { type: 'text', value: ing.insumo_name, placeholder: 'Insumo', list: 'all-insumos' });
          nameInput.addEventListener('input', () => {
            ing.insumo_name = nameInput.value;
            const match = STATE.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
            ing.insumo_id = match ? match.id : slugify(nameInput.value);
          });
          row.appendChild(nameInput);
          const qtyInput = el('input', { type: 'number', step: '0.01', value: ing.qty != null ? ing.qty : '', placeholder: 'Qtd' });
          qtyInput.addEventListener('input', () => {
            const v = parseFloat(qtyInput.value);
            ing.qty = isNaN(v) ? null : v;
            ing.qty_raw = qtyInput.value;
            ing.is_qb = qtyInput.value.toLowerCase() === 'q.b';
          });
          row.appendChild(qtyInput);
          const unitInput = el('input', { type: 'text', value: ing.unit || '', placeholder: 'g' });
          unitInput.addEventListener('input', () => { ing.unit = unitInput.value; });
          row.appendChild(unitInput);
          const obsInput = el('input', { type: 'text', value: ing.observacao || '', placeholder: 'Observação' });
          obsInput.addEventListener('input', () => { ing.observacao = obsInput.value; });
          row.appendChild(obsInput);
          const fcInput = el('input', { type: 'number', step: '0.01', min: '1', placeholder: 'FC', value: ing.fc || '', title: 'Fator de correção' });
          fcInput.addEventListener('input', () => {
            const v = parseFloat(fcInput.value);
            if (!v || v <= 1) delete ing.fc;
            else ing.fc = v;
          });
          row.appendChild(fcInput);
          row.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: () => { sf.ingredientes.splice(ingIdx, 1); renderIngs(); } }, '×'));
          ingList.appendChild(row);
        });
        ingList.appendChild(el('button', { class: 'btn btn-small', onclick: () => {
          sf.ingredientes.push({ insumo_id: '', insumo_name: '', qty: null, qty_raw: '', unit: 'g', observacao: '', is_qb: false });
          renderIngs();
        } }, '+ adicionar insumo'));
      }
      renderIngs();
      box.appendChild(ingList);
      box.appendChild(el('label', { class: 'field' },
        el('span', { class: 'label-text' }, 'Modo de Preparo'),
        (() => { const ta = el('textarea', {}, sf.modo_preparo || ''); ta.addEventListener('input', () => sf.modo_preparo = ta.value); return ta; })()
      ));
      sfWrap.appendChild(box);
    });
    sfWrap.appendChild(el('button', { class: 'btn', onclick: () => {
      dish.sub_fichas.push({ id: uid(), name: `Preparação ${dish.sub_fichas.length + 1}`, rendimento: '', ingredientes: [], modo_preparo: '' });
      renderSubfichas();
    } }, '+ Adicionar sub-ficha'));
  }
  renderSubfichas();
  panel.appendChild(sfWrap);

  const dl = el('datalist', { id: 'all-insumos' });
  STATE.insumos.forEach(i => dl.appendChild(el('option', { value: i.name })));
  panel.appendChild(dl);

  panel.appendChild(el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn btn-primary', onclick: () => saveDishAction(cid, dish, dishId) }, 'Salvar'),
    el('a', { class: 'btn', href: `#/c/${cid}/admin` }, 'Cancelar')
  ));
  app.appendChild(panel);
}

async function saveDishAction(cid, dish, originalId) {
  if (!dish.name.trim()) { alert('Nome do prato é obrigatório'); return; }
  const newId = slugify(dish.name);
  dish.id = newId;
  try {
    // Register new insumos
    const batch = writeBatch(db);
    let bc = 0;
    for (const sf of dish.sub_fichas) {
      for (const ing of sf.ingredientes) {
        if (!ing.insumo_name.trim()) continue;
        const existing = STATE.insumos.find(i => i.name.toLowerCase() === ing.insumo_name.toLowerCase());
        if (!existing) {
          const newIns = { id: slugify(ing.insumo_name), name: ing.insumo_name.trim(), unit: ing.unit || 'g', price: 0 };
          ing.insumo_id = newIns.id;
          const clean = { ...newIns }; delete clean.id;
          batch.set(insumoDoc(cid, newIns.id), clean);
          bc++;
        } else {
          ing.insumo_id = existing.id;
        }
      }
    }
    if (bc > 0) await batch.commit();
    if (originalId && originalId !== newId) await deleteDish(cid, originalId);
    await saveDish(cid, dish);
    toast('Salvo');
    location.hash = `#/c/${cid}/ficha/${dish.id}`;
  } catch (err) { console.error(err); toast('Erro: ' + err.message); }
}

function fieldInput(label, type, value, onChange) {
  const wrap = el('label', { class: 'field' }, el('span', { class: 'label-text' }, label));
  const input = el('input', { type, value: value ?? '' });
  input.addEventListener('input', () => onChange(input.value));
  wrap.appendChild(input);
  return wrap;
}

// ---------- Exports ----------
function exportFichaPDF(dish, state) {
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const margin = 15;
  let y = margin;
  const pageWidth = docPdf.internal.pageSize.getWidth();
  const contentWidth = pageWidth - margin * 2;
  const isTrabalho = state.view === 'trabalho';
  const all = dishCost(dish);
  const currentSf = (dish.sub_fichas || []).find(s => s.id === state.activeSf) || (dish.sub_fichas || [])[0];
  const sf = state.view === 'trabalho' ? currentSf : null;

  docPdf.setFont('times', 'italic').setFontSize(10).setTextColor(130);
  docPdf.text(isTrabalho ? 'Ficha Técnica Operacional' : 'Ficha de Custo por Rendimento', pageWidth / 2, y, { align: 'center' });
  y += 7;
  docPdf.setFont('times', 'normal').setFontSize(22).setTextColor(20);
  docPdf.text(dish.name, pageWidth / 2, y, { align: 'center' });
  y += 10;
  docPdf.setDrawColor(184, 149, 94).setLineWidth(0.3);
  docPdf.line(pageWidth / 2 - 20, y, pageWidth / 2 + 20, y);
  y += 8;

  // Cascade scales (dish-level scaling)
  const scales = typeof state.scales === 'function' ? state.scales() : {};
  const sfScale = scales[currentSf?.id] || 1;
  const finalSf2 = dish.sub_fichas[dish.sub_fichas.length - 1];
  const scaleChanged = sfScale !== 1;
  if (isTrabalho) {
    docPdf.setFont('times', 'normal').setFontSize(14).setTextColor(20);
    docPdf.text(sf.name, margin, y); y += 5;
    if (sf.rendimento) {
      docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(120);
      const rendStr = formatRendimento(sf.rendimento, sfScale) + (scaleChanged ? ` (escalado ${fmtNum(sfScale, 2)}×)` : '');
      docPdf.text('Rendimento: ' + rendStr, margin, y); y += 6;
    }
    docPdf.autoTable({
      startY: y, margin: { left: margin, right: margin },
      head: [['Insumo', 'Quantidade', 'Unidade', 'Observação']],
      body: (sf.ingredientes || []).map(i => {
        const f = formatIngQty(i, sfScale);
        return [
          i.insumo_name,
          f.text,
          f.unit || '—', i.observacao || '—'
        ];
      }),
      theme: 'plain',
      headStyles: { fillColor: [247, 245, 238], textColor: [80, 80, 80], fontStyle: 'bold', fontSize: 8, cellPadding: 3 },
      bodyStyles: { fontSize: 9, cellPadding: 2.5, textColor: [40, 40, 40] },
      alternateRowStyles: { fillColor: [252, 251, 247] }
    });
    y = docPdf.lastAutoTable.finalY + 8;
    if (sf.modo_preparo) {
      if (y > 250) { docPdf.addPage(); y = margin; }
      docPdf.setFont('helvetica', 'bold').setFontSize(9).setTextColor(100);
      docPdf.text('MODO DE PREPARO', margin, y); y += 5;
      docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(40);
      const lines = docPdf.splitTextToSize(sf.modo_preparo.replace(/\s+/g, ' '), contentWidth);
      docPdf.text(lines, margin, y); y += lines.length * 4 + 6;
    }
    if (dish.louca) {
      if (y > 260) { docPdf.addPage(); y = margin; }
      docPdf.setFont('helvetica', 'bold').setFontSize(9).setTextColor(100);
      docPdf.text('APRESENTAÇÃO / LOUÇA', margin, y); y += 5;
      docPdf.setFont('helvetica', 'normal').setTextColor(40);
      const lines = docPdf.splitTextToSize(dish.louca, contentWidth);
      docPdf.text(lines, margin, y); y += lines.length * 4 + 5;
    }
    if (dish.equipamentos && dish.equipamentos.length) {
      if (y > 260) { docPdf.addPage(); y = margin; }
      docPdf.setFont('helvetica', 'bold').setFontSize(9).setTextColor(100);
      docPdf.text('EQUIPAMENTOS', margin, y); y += 5;
      docPdf.setFont('helvetica', 'normal').setTextColor(40);
      const lines = docPdf.splitTextToSize(dish.equipamentos.join(' · '), contentWidth);
      docPdf.text(lines, margin, y);
    }
  } else {
    all.sfCosts.forEach(({ sf: x, rows, total }) => {
      if (y > 240) { docPdf.addPage(); y = margin; }
      docPdf.setFont('times', 'normal').setFontSize(12).setTextColor(20);
      docPdf.text(`${x.name}${x.rendimento ? ' — ' + x.rendimento : ''}`, margin, y); y += 4;
      docPdf.autoTable({
        startY: y, margin: { left: margin, right: margin },
        head: [['Insumo', 'Qtd', 'Un.', 'Preço unit.', 'Custo']],
        body: rows.map(({ ing, insumo, cost, isSubref, subSf }) => {
          const f = formatIngQty(ing);
          const np = insumo ? normalizePriceForDisplay(insumo) : null;
          return [
            (isSubref ? '↪ ' : '') + ing.insumo_name,
            f.text, f.unit || '—',
            isSubref ? `sub-ficha (${formatRendimento(subSf?.rendimento || '')})`
                     : (np ? `${fmtBRL(np.price)}/${np.unit}` : '—'),
            fmtBRL(cost)
          ];
        }),
        foot: [['', '', '', 'Subtotal', fmtBRL(total)]],
        theme: 'plain',
        headStyles: { fillColor: [247, 245, 238], textColor: [80, 80, 80], fontStyle: 'bold', fontSize: 8, cellPadding: 2.5 },
        bodyStyles: { fontSize: 8.5, cellPadding: 2, textColor: [40, 40, 40] },
        footStyles: { fontStyle: 'bold', fillColor: [252, 251, 247], fontSize: 9 },
        columnStyles: { 1: { halign: 'right' }, 3: { halign: 'right' }, 4: { halign: 'right' } }
      });
      y = docPdf.lastAutoTable.finalY + 6;
    });
    if (y > 240) { docPdf.addPage(); y = margin; }
    docPdf.setFillColor(26, 26, 26).rect(margin, y, contentWidth, 28, 'F');
    docPdf.setTextColor(255).setFont('helvetica', 'normal').setFontSize(8);
    const col = contentWidth / 3;
    docPdf.text('CUSTO TOTAL', margin + 5, y + 6);
    docPdf.text('CUSTO/PORÇÃO', margin + col + 5, y + 6);
    docPdf.text('PREÇO SUGERIDO', margin + col * 2 + 5, y + 6);
    docPdf.setFont('times', 'normal').setFontSize(16);
    docPdf.text(fmtBRL(all.total), margin + 5, y + 18);
    docPdf.setTextColor(216, 184, 120);
    docPdf.text(fmtBRL(all.costPerPortion), margin + col + 5, y + 18);
    docPdf.setTextColor(255);
    docPdf.text(fmtBRL(all.suggestedPrice), margin + col * 2 + 5, y + 18);
    y += 32;
    docPdf.setTextColor(80).setFont('helvetica', 'normal').setFontSize(9);
    docPdf.text(`Markup: ${fmtNum(all.markup, 0)}%   ·   CMV: ${fmtNum(all.cmv, 1)}%   ·   Porções: ${all.portions}`, margin, y);
  }
  const suffix = scaleChanged ? `-escalado${fmtNum(sfScale, 2)}x` : '';
  docPdf.save(`${slugify(dish.name)}-${isTrabalho ? 'trabalho' : 'custo'}${suffix}.pdf`);
}

function exportFichaXLSX(dish, state) {
  const wb = XLSX.utils.book_new();
  const all = dishCost(dish);
  const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
  // Cascade scales (dish-level)
  const scales = typeof state?.scales === 'function' ? state.scales() : {};
  const finalScale = scales[lastSf?.id] || 1;
  const scaleChanged = finalScale !== 1;
  const summary = [
    ['FICHA TÉCNICA'],
    ['Prato', dish.name],
    ['Rendimento final', (lastSf?.rendimento || '—') + (scaleChanged ? ` (escalado ${fmtNum(finalScale, 2)}x)` : '')],
    [],
    ['Custo total', all.total],
    ['Porções', all.portions],
    ['Custo por porção', all.costPerPortion],
    ['Markup (%)', all.markup],
    ['Preço de venda sugerido', all.suggestedPrice],
    ['CMV (%)', all.cmv],
    [],
    ['Louça', dish.louca || '—'],
    ['Equipamentos', (dish.equipamentos || []).join('; ')]
  ];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(summary), 'Resumo');
  all.sfCosts.forEach(({ sf, rows, total }, idx) => {
    const data = [
      [sf.name], ['Rendimento', formatRendimento(sf.rendimento || '')], [],
      ['Insumo', 'Quantidade', 'Unidade', 'Observação', 'Preço unit.', 'Custo'],
      ...rows.map(({ ing, insumo, cost }) => {
        const f = formatIngQty(ing);
        const np = insumo ? normalizePriceForDisplay(insumo) : null;
        return [
          ing.insumo_name,
          ing.is_qb ? 'Q.B' : (f.text || ''),
          f.unit || '', ing.observacao || '',
          np ? np.price : 0, cost
        ];
      }),
      [], ['Subtotal', '', '', '', '', total], [],
      ['Modo de preparo:'], [sf.modo_preparo || '']
    ];
    const name = ('SF' + (idx + 1) + '-' + sf.name).replace(/[/\\?*\[\]:]/g, '').slice(0, 30);
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(data), name);
  });
  // Se escala foi alterada, adiciona uma aba "Escalada" com TODAS as sub-fichas em escala
  if (scaleChanged && state?.view === 'trabalho') {
    const scaledAll = [[`PRODUÇÃO ESCALADA — alvo final ${fmtNum(state.finalTargetQty, 2)} ${state.finalUnit}`], []];
    dish.sub_fichas.forEach(s => {
      const sScale = scales[s.id] || 1;
      scaledAll.push([s.name + (sScale !== 1 ? ` (× ${fmtNum(sScale, 2)})` : '')]);
      scaledAll.push(['Rendimento', formatRendimento(s.rendimento, sScale)]);
      scaledAll.push([]);
      scaledAll.push(['Insumo', 'Quantidade', 'Unidade', 'Observação']);
      (s.ingredientes || []).forEach(i => {
        const f = formatIngQty(i, sScale);
        scaledAll.push([i.insumo_name, f.text || '', f.unit || '', i.observacao || '']);
      });
      scaledAll.push([]);
    });
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(scaledAll), 'Escalada');
  }
  const suffix = scaleChanged ? `-esc${fmtNum(finalScale, 2)}x` : '';
  XLSX.writeFile(wb, `${slugify(dish.name)}${suffix}.xlsx`);
}

// ---------- Init ----------
onAuthStateChanged(auth, async (user) => {
  STATE.user = user;
  if (user) await ensureUserDoc(user);
  else STATE.userDoc = null;
  updateAuthUI();
  route();
});

// Wire up UI
$('#btn-auth').addEventListener('click', () => {
  if (STATE.user) doLogout();
  else openLoginModal();
});
$('#login-form').addEventListener('submit', e => {
  e.preventDefault();
  doLogin($('#login-email').value, $('#login-password').value);
});
$('#login-cancel').addEventListener('click', closeLoginModal);
$('.modal-overlay').addEventListener('click', closeLoginModal);

// Optional: download backup of current cliente (admin only)
const btnBackup = $('#btn-download-backup');
if (btnBackup) btnBackup.addEventListener('click', () => {
  if (!STATE.currentClienteId) { toast('Abra um cliente antes'); return; }
  const payload = {
    cliente: STATE.currentCliente,
    dishes: STATE.dishes, insumos: STATE.insumos,
    equipamentos_disponiveis: STATE.equipamentos
  };
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = `backup-${STATE.currentClienteId}-${new Date().toISOString().slice(0,10)}.json`;
  a.click(); URL.revokeObjectURL(url);
  toast('Backup baixado');
});
