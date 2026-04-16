/* ================================================================
   Fichas Técnicas — single-page app (Firebase + vanilla JS)
   ================================================================ */

import { initializeApp } from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js';
import {
  getFirestore, collection, doc, setDoc, deleteDoc, onSnapshot, getDoc, getDocs, writeBatch
} from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js';
import {
  getAuth, signInWithEmailAndPassword, onAuthStateChanged, signOut
} from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-auth.js';

// ---------- Firebase config (safe to be public — security comes from rules) ----------
const firebaseConfig = {
  apiKey: "AIzaSyDF3jKFty1lQrib9lwsRoRxBFTsO-2boMY",
  authDomain: "fichastecnicas-c3829.firebaseapp.com",
  projectId: "fichastecnicas-c3829",
  storageBucket: "fichastecnicas-c3829.firebasestorage.app",
  messagingSenderId: "461159721803",
  appId: "1:461159721803:web:922a9a8a39f4c5ec2416ce"
};
const ADMIN_EMAIL = "gustavo@quibebe.com.br";

const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);
const auth = getAuth(firebaseApp);

// ---------- State ----------
const DATA = {
  dishes: [],
  insumos: [],
  equipamentos_disponiveis: [],
  loaded: { dishes: false, insumos: false, config: false }
};
let currentUser = null;
let isAdmin = false;

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
const slugify = s => (s || '').toString().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') || 'item';
const uid = () => Math.random().toString(36).slice(2, 9);

function toast(msg) {
  const t = $('#toast');
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(t._to);
  t._to = setTimeout(() => { t.hidden = true; }, 2400);
}

// ---------- Debouncing for Firestore writes ----------
const pendingWrites = new Map();
function scheduleSave(key, saveFn, delay = 800) {
  const existing = pendingWrites.get(key);
  if (existing) clearTimeout(existing);
  pendingWrites.set(key, setTimeout(async () => {
    pendingWrites.delete(key);
    try { await saveFn(); } catch (e) { console.error(e); toast('Erro ao salvar: ' + e.message); }
  }, delay));
}

// ---------- Firestore I/O ----------
async function saveDishToFirestore(dish) {
  if (!isAdmin) { toast('Faça login como admin para editar'); return; }
  const clean = { ...dish };
  delete clean.id; // id goes in doc path
  await setDoc(doc(db, 'dishes', dish.id), clean);
}
async function deleteDishFromFirestore(dishId) {
  if (!isAdmin) return;
  await deleteDoc(doc(db, 'dishes', dishId));
}
async function saveInsumoToFirestore(insumo) {
  if (!isAdmin) return;
  const clean = { ...insumo };
  delete clean.id;
  await setDoc(doc(db, 'insumos', insumo.id), clean);
}
async function deleteInsumoFromFirestore(id) {
  if (!isAdmin) return;
  await deleteDoc(doc(db, 'insumos', id));
}

// ---------- Real-time listeners ----------
function setupListeners() {
  onSnapshot(collection(db, 'dishes'), (snap) => {
    DATA.dishes = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    // Sort by name
    DATA.dishes.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    DATA.loaded.dishes = true;
    maybeRender();
  });
  onSnapshot(collection(db, 'insumos'), (snap) => {
    DATA.insumos = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    DATA.insumos.sort((a, b) => (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase()));
    DATA.loaded.insumos = true;
    maybeRender();
  });
  onSnapshot(doc(db, 'config', 'equipamentos'), (snap) => {
    DATA.equipamentos_disponiveis = snap.exists() ? (snap.data().list || []) : [];
    DATA.loaded.config = true;
    maybeRender();
  });
}

let hasRenderedOnce = false;
function maybeRender() {
  if (DATA.loaded.dishes && DATA.loaded.insumos) {
    if (!hasRenderedOnce) hasRenderedOnce = true;
    route();
  }
}

// ---------- Seed initial data from data.json ----------
async function seedFromJson() {
  if (!isAdmin) { toast('Faça login como admin'); return; }
  const wipe = confirm('Importar dados iniciais do arquivo data.json?\n\n• OK = limpa o banco atual e importa tudo do zero (recomendado se quer preservar apenas os preços já inseridos — eles serão perdidos)\n• Cancelar = não importa');
  if (!wipe) return;
  try {
    toast('Importando...');
    const resp = await fetch('data/data.json');
    const seed = await resp.json();

    // Collect existing IDs to compare with seed (to delete orphans)
    const [existingDishesSnap, existingInsumosSnap] = await Promise.all([
      getDocs(collection(db, 'dishes')),
      getDocs(collection(db, 'insumos'))
    ]);
    const seedDishIds = new Set(seed.dishes.map(d => d.id));
    const seedInsumoIds = new Set(seed.insumos.map(i => i.id));

    let batch = writeBatch(db);
    let count = 0;
    const flush = async () => {
      if (count > 0) { await batch.commit(); batch = writeBatch(db); count = 0; }
    };
    const add = (op, ref, data) => {
      if (op === 'set') batch.set(ref, data);
      else batch.delete(ref);
      count++;
      if (count >= 400) return flush();
    };

    // Delete orphans
    for (const d of existingDishesSnap.docs) {
      if (!seedDishIds.has(d.id)) await add('delete', doc(db, 'dishes', d.id));
    }
    for (const d of existingInsumosSnap.docs) {
      if (!seedInsumoIds.has(d.id)) await add('delete', doc(db, 'insumos', d.id));
    }
    // Keep existing prices when re-seeding (merge)
    const existingPrices = {};
    existingInsumosSnap.docs.forEach(d => { existingPrices[d.id] = d.data().price || 0; });

    for (const dish of seed.dishes) {
      const clean = { ...dish };
      delete clean.id;
      await add('set', doc(db, 'dishes', dish.id), clean);
    }
    for (const ins of seed.insumos) {
      const clean = { ...ins };
      delete clean.id;
      // Preserve existing price if available
      if (existingPrices[ins.id] != null) clean.price = existingPrices[ins.id];
      await add('set', doc(db, 'insumos', ins.id), clean);
    }
    await add('set', doc(db, 'config', 'equipamentos'), { list: seed.equipamentos_disponiveis || [] });
    await flush();
    toast('Dados importados! Preços existentes preservados.');
  } catch (err) {
    console.error(err);
    toast('Erro ao importar: ' + err.message);
  }
}

// ---------- Backup / Restore ----------
function downloadBackup() {
  const blob = new Blob([JSON.stringify({
    version: 1,
    dishes: DATA.dishes,
    insumos: DATA.insumos,
    equipamentos_disponiveis: DATA.equipamentos_disponiveis
  }, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `fichas-backup-${new Date().toISOString().slice(0,10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
  toast('Backup baixado');
}

function restoreBackup(file) {
  if (!isAdmin) { toast('Apenas admin pode restaurar'); return; }
  const reader = new FileReader();
  reader.onload = async (e) => {
    try {
      const d = JSON.parse(e.target.result);
      if (!d.dishes || !d.insumos) throw new Error('formato inválido');
      if (!confirm('Restaurar backup? Sobrescreve tudo no banco.')) return;
      toast('Restaurando...');
      let batch = writeBatch(db);
      let count = 0;
      for (const dish of d.dishes) {
        const clean = { ...dish };
        delete clean.id;
        batch.set(doc(db, 'dishes', dish.id), clean);
        count++;
        if (count >= 400) { await batch.commit(); batch = writeBatch(db); count = 0; }
      }
      for (const ins of d.insumos) {
        const clean = { ...ins };
        delete clean.id;
        batch.set(doc(db, 'insumos', ins.id), clean);
        count++;
        if (count >= 400) { await batch.commit(); batch = writeBatch(db); count = 0; }
      }
      if (d.equipamentos_disponiveis) {
        batch.set(doc(db, 'config', 'equipamentos'), { list: d.equipamentos_disponiveis });
      }
      if (count > 0) await batch.commit();
      toast('Backup restaurado');
    } catch (err) { alert('Erro: ' + err.message); }
  };
  reader.readAsText(file);
}

// ---------- Auth ----------
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
  toast('Saiu');
  if (location.hash.startsWith('#/admin')) location.hash = '#/';
}

function updateAuthUI() {
  const authBtn = $('#btn-auth');
  const navAdmin = $('#nav-admin');
  if (isAdmin) {
    authBtn.textContent = 'Sair';
    authBtn.classList.add('logged-in');
    navAdmin.style.display = '';
    $$('.admin-only').forEach(el => el.style.display = '');
  } else {
    authBtn.textContent = 'Entrar';
    authBtn.classList.remove('logged-in');
    navAdmin.style.display = 'none';
    $$('.admin-only').forEach(el => el.style.display = 'none');
  }
}

// ---------- Cost calculations ----------
function findInsumo(id) { return DATA.insumos.find(i => i.id === id); }

// Normalize a string for sub-ficha reference matching
function nrm(s) {
  if (!s) return '';
  return String(s).trim().toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\(.*?\)/g, '').replace(/\s+/g, ' ').trim().replace(/[,.;:\s]+$/, '');
}
// Runtime detection: find if ingredient name matches an earlier sub-ficha in the dish
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

// Parse rendimento string to { qty, unit } for proportion calculations
function parseRendimentoQty(rendStr) {
  if (!rendStr) return { qty: 1, unit: '' };
  const s = String(rendStr).trim();
  const m = s.match(/^(\d+(?:[.,]\d+)?)\s*([a-zA-Zµ/]*)/);
  if (m) {
    const qty = parseFloat(m[1].replace(',', '.'));
    const unit = (m[2] || '').toLowerCase();
    return { qty, unit };
  }
  return { qty: 1, unit: '' };
}

// Calculate sub-ficha cost with optional sub-ficha references (no double-counting).
// Memoize results per dish.sub_fichas to avoid recalc.
function subfichaCost(sf, dish, cache = null, visited = null) {
  cache = cache || new Map();
  visited = visited || new Set();
  if (cache.has(sf.id)) return cache.get(sf.id);
  if (visited.has(sf.id)) return { rows: [], total: 0 };
  visited.add(sf.id);

  const sfIdx = dish.sub_fichas.findIndex(s => s.id === sf.id);
  let total = 0;
  const rows = (sf.ingredientes || []).map(ing => {
    // Determine subref: explicit subref_id OR runtime detection
    let subSf = null;
    if (ing.subref_id) {
      subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
    }
    if (!subSf && sfIdx > 0) {
      subSf = detectSubref(dish, sfIdx, ing.insumo_name);
    }
    if (subSf) {
      // Proportional cost from the referenced sub-ficha
      const subResult = subfichaCost(subSf, dish, cache, new Set(visited));
      const subRend = parseRendimentoQty(subSf.rendimento);
      const ingQty = ing.qty;
      let cost = 0;
      if (ingQty != null && subRend.qty > 0) {
        // Normalize units: convert ingredient qty to sub-ficha rendimento's unit if possible
        const [qConverted] = normalizeSubrefQty(ingQty, ing.unit, subRend.unit);
        cost = (qConverted / subRend.qty) * subResult.total;
      }
      total += cost;
      return { ing, insumo: null, cost, subSf, isSubref: true };
    }
    // Real insumo
    const insumo = findInsumo(ing.insumo_id);
    const [qNorm, priceNorm] = normalizeQtyPrice(ing, insumo);
    const cost = (qNorm != null && priceNorm != null) ? qNorm * priceNorm : 0;
    total += cost;
    return { ing, insumo, cost, isSubref: false };
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
  // Dish total = cost of the FINAL sub-ficha (propagates references, no double-count)
  const finalSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
  const finalResult = finalSf ? sfCosts.find(x => x.sf.id === finalSf.id) : null;
  const total = finalResult ? finalResult.total : 0;
  const portions = parsePortions(finalSf ? finalSf.rendimento : '');
  const costPerPortion = portions > 0 ? total / portions : 0;
  const markup = dish.markup || 300;
  const suggestedPrice = costPerPortion * (1 + markup / 100);
  const cmv = suggestedPrice > 0 ? (costPerPortion / suggestedPrice) * 100 : 0;
  return { sfCosts, total, portions, costPerPortion, suggestedPrice, cmv };
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

// ---------- Router ----------
function route() {
  const hash = location.hash.slice(1) || '/';
  const parts = hash.split('/').filter(Boolean);
  $$('.site-nav a').forEach(a => a.classList.remove('active'));
  // If not loaded yet, keep loading state
  if (!DATA.loaded.dishes || !DATA.loaded.insumos) return;
  // Empty database state
  if (DATA.dishes.length === 0 && DATA.insumos.length === 0) {
    renderEmptyState();
    return;
  }
  if (parts.length === 0) {
    $('[data-nav="home"]').classList.add('active');
    renderHome();
  } else if (parts[0] === 'ficha' && parts[1]) {
    renderFicha(parts[1]);
  } else if (parts[0] === 'insumos') {
    $('[data-nav="insumos"]').classList.add('active');
    renderInsumos();
  } else if (parts[0] === 'admin') {
    if (!isAdmin) { openLoginModal(); renderHome(); return; }
    $('[data-nav="admin"]').classList.add('active');
    if (parts[1] === 'edit' && parts[2]) renderAdminEdit(parts[2]);
    else if (parts[1] === 'new') renderAdminEdit(null);
    else renderAdminList();
  } else {
    renderHome();
  }
  window.scrollTo(0, 0);
}
window.addEventListener('hashchange', route);

// ---------- Empty state ----------
function renderEmptyState() {
  const app = $('#app');
  app.innerHTML = '';
  const box = el('div', { style: 'max-width:600px;margin:4rem auto;text-align:center;' },
    el('h1', {}, 'Banco de dados vazio'),
    el('p', { style: 'color:#888;font-style:italic;font-family:"Cormorant Garamond",serif;font-size:1.2rem;margin-bottom:2rem;' },
      'As fichas ainda não foram importadas para o banco.'),
    isAdmin
      ? el('button', { class: 'btn btn-primary', onclick: seedFromJson }, 'Importar dados iniciais agora')
      : el('div', {},
          el('p', {}, 'Entre como administrador para importar os dados iniciais.'),
          el('button', { class: 'btn btn-primary', onclick: openLoginModal }, 'Entrar como admin')
        )
  );
  app.appendChild(box);
}

// ---------- Views ----------
function renderHome() {
  const app = $('#app');
  app.innerHTML = '';
  const totalSubfichas = DATA.dishes.reduce((s, d) => s + (d.sub_fichas || []).length, 0);
  const hero = el('section', { class: 'home-hero' },
    el('h1', {}, 'Cardápio'),
    el('p', { class: 'subtitle' }, 'Fichas técnicas operacionais e custeio por rendimento'),
    el('div', { class: 'home-stats' },
      el('span', {}, el('strong', {}, DATA.dishes.length.toString()), 'Pratos'),
      el('span', {}, el('strong', {}, totalSubfichas.toString()), 'Sub-fichas'),
      el('span', {}, el('strong', {}, DATA.insumos.length.toString()), 'Insumos'),
    )
  );
  app.appendChild(hero);

  const grid = el('div', { class: 'grid-dishes' });
  DATA.dishes.forEach(dish => {
    const { costPerPortion } = dishCost(dish);
    const photo = dish.photos && dish.photos[0];
    const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
    const card = el('a', { class: 'card-dish', href: `#/ficha/${dish.id}` },
      el('div', { class: 'card-photo' },
        photo ? el('img', { src: photo, alt: dish.name }) : el('span', { class: 'placeholder' }, 'sem foto')
      ),
      el('div', { class: 'card-body' },
        el('h3', {}, dish.name),
        el('div', { class: 'card-meta' },
          el('span', {}, 'Rendimento', el('br'), el('strong', {}, lastSf?.rendimento || '—')),
          el('span', {}, 'Custo/porção', el('br'), el('strong', {}, fmtBRL(costPerPortion)))
        )
      )
    );
    grid.appendChild(card);
  });
  app.appendChild(grid);
}

function renderFicha(dishId) {
  const dish = DATA.dishes.find(d => d.id === dishId);
  const app = $('#app');
  app.innerHTML = '';
  if (!dish) {
    app.appendChild(el('p', {}, 'Prato não encontrado. ', el('a', { href: '#/' }, 'Voltar')));
    return;
  }

  const state = {
    view: 'trabalho',
    activeSf: (dish.sub_fichas || [])[dish.sub_fichas.length - 1]?.id || (dish.sub_fichas || [])[0]?.id,
  };

  const header = el('div', { class: 'ficha-header' },
    el('a', { class: 'back-link', href: '#/' }, '← Voltar ao cardápio'),
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

  const tabs = el('div', { class: 'subficha-tabs' });
  (dish.sub_fichas || []).forEach(sf => {
    tabs.appendChild(el('button', { 'data-sf': sf.id }, sf.name));
  });
  app.appendChild(tabs);

  const body = el('div', {});
  app.appendChild(body);

  const actions = el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn', onclick: () => exportFichaPDF(dish, state) }, 'Exportar PDF'),
    el('button', { class: 'btn', onclick: () => exportFichaXLSX(dish) }, 'Exportar Excel')
  );
  app.appendChild(actions);

  function updateUI() {
    $$('.view-toggle button', toggle).forEach(b => b.classList.toggle('active', b.dataset.view === state.view));
    $$('.subficha-tabs button', tabs).forEach(b => b.classList.toggle('active', b.dataset.sf === state.activeSf));
    const sf = (dish.sub_fichas || []).find(s => s.id === state.activeSf) || (dish.sub_fichas || [])[0];
    body.innerHTML = '';
    if (state.view === 'trabalho') body.appendChild(renderFichaTrabalho(dish, sf));
    else body.appendChild(renderFichaCusto(dish, sf));
  }
  toggle.addEventListener('click', e => {
    if (e.target.dataset.view) { state.view = e.target.dataset.view; updateUI(); }
  });
  tabs.addEventListener('click', e => {
    if (e.target.dataset.sf) { state.activeSf = e.target.dataset.sf; updateUI(); }
  });
  updateUI();
}

function renderFichaTrabalho(dish, sf) {
  const wrap = el('div', { class: 'ficha-body' });
  wrap.appendChild(el('div', { class: 'ficha-meta-line' }, 'Ficha Técnica Operacional'));
  wrap.appendChild(el('h2', {}, sf.name));
  if (sf.rendimento) wrap.appendChild(el('div', { class: 'ficha-meta-line' }, 'Rendimento: ', el('strong', {}, sf.rendimento)));

  const sfIdx = dish.sub_fichas.findIndex(s => s.id === sf.id);
  const tbl = el('table', { class: 'ficha-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', { class: 'num' }, 'Quantidade'),
      el('th', {}, 'Unidade'),
      el('th', {}, 'Observação / Processamento')
    )),
    el('tbody', {}, ...(sf.ingredientes || []).map(ing => {
      // Detect if this ingredient is a sub-ficha reference
      let subSf = null;
      if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
      if (!subSf && sfIdx > 0) subSf = detectSubref(dish, sfIdx, ing.insumo_name);
      const nameCell = subSf
        ? el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↪ '),
            ing.insumo_name,
            el('span', { class: 'subref-note' }, ' (ver preparação)')
          )
        : el('td', { 'data-label': 'Insumo' }, ing.insumo_name);
      return el('tr', { class: subSf ? 'row-subref' : '' },
        nameCell,
        el('td', { class: 'num', 'data-label': 'Quantidade' }, ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—'))),
        el('td', { 'data-label': 'Unidade' }, ing.unit || '—'),
        el('td', { 'data-label': 'Observação' }, ing.observacao || '—')
      );
    }))
  );
  wrap.appendChild(tbl);

  if (sf.modo_preparo) {
    const sec = el('div', { class: 'ficha-section' },
      el('h3', {}, 'Modo de Preparo'),
      el('div', { class: 'modo-preparo' }, sf.modo_preparo)
    );
    wrap.appendChild(sec);
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

function renderFichaCusto(dish, currentSf) {
  const wrap = el('div', { class: 'ficha-body' });
  wrap.appendChild(el('div', { class: 'ficha-meta-line' }, 'Ficha de Custo por Rendimento'));
  wrap.appendChild(el('h2', {}, dish.name));

  const all = dishCost(dish);
  all.sfCosts.forEach(({ sf, rows, total }) => {
    const section = el('div', { class: 'ficha-section' },
      el('h3', {}, sf.name + (sf.rendimento ? ` — rendimento: ${sf.rendimento}` : ''))
    );
    const tbl = el('table', { class: 'ficha-table' },
      el('thead', {}, el('tr', {},
        el('th', {}, 'Insumo'),
        el('th', { class: 'num' }, 'Quantidade'),
        el('th', {}, 'Unidade'),
        el('th', { class: 'num' }, 'Preço unit.'),
        el('th', { class: 'num' }, 'Custo')
      )),
      el('tbody', {}, ...rows.map(({ ing, insumo, cost, isSubref, subSf }) => {
        const qtyTxt = ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—'));
        let priceTxt;
        let nameCell;
        if (isSubref && subSf) {
          // Sub-ficha reference — different display
          const subRend = parseRendimentoQty(subSf.rendimento);
          priceTxt = el('span', { class: 'subref-note' }, `sub-ficha (${subSf.rendimento || '—'})`);
          nameCell = el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↪ '),
            ing.insumo_name
          );
        } else {
          priceTxt = insumo ? `${fmtBRL(insumo.price || 0)} / ${insumo.unit || '—'}` : '—';
          nameCell = el('td', { 'data-label': 'Insumo' }, ing.insumo_name);
        }
        return el('tr', { class: isSubref ? 'row-subref' : '' },
          nameCell,
          el('td', { class: 'num', 'data-label': 'Quantidade' }, qtyTxt),
          el('td', { 'data-label': 'Unidade' }, ing.unit || '—'),
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
    el('div', {},
      el('span', { class: 'stat-label' }, 'Custo total do prato'),
      el('span', { class: 'stat-value' }, fmtBRL(all.total))
    ),
    el('div', {},
      el('span', { class: 'stat-label' }, 'Porções (aprox.)'),
      el('span', { class: 'stat-value' }, fmtNum(all.portions, 0))
    ),
    el('div', {},
      el('span', { class: 'stat-label' }, 'Custo por porção'),
      el('span', { class: 'stat-value gold' }, fmtBRL(all.costPerPortion))
    )
  );
  wrap.appendChild(total);

  // Markup + CMV editor
  const cost = el('div', { class: 'cost-summary' });
  const markupInput = el('input', { type: 'number', min: '0', step: '5', value: String(dish.markup || 300) });
  if (!isAdmin) markupInput.disabled = true;
  cost.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'Markup (%)'),
    markupInput
  ));
  const priceSpan = el('span', { class: 'stat-value accent' }, fmtBRL(all.suggestedPrice));
  cost.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'Preço de venda sugerido'),
    priceSpan
  ));
  const cmvSpan = el('span', { class: 'stat-value' }, fmtNum(all.cmv, 1) + '%');
  cost.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'CMV (food cost)'),
    cmvSpan
  ));
  markupInput.addEventListener('input', () => {
    const newMarkup = parseFloat(markupInput.value) || 0;
    dish.markup = newMarkup;
    const newPrice = all.costPerPortion * (1 + newMarkup / 100);
    priceSpan.textContent = fmtBRL(newPrice);
    cmvSpan.textContent = newPrice > 0 ? fmtNum(all.costPerPortion / newPrice * 100, 1) + '%' : '—';
    scheduleSave('dish-' + dish.id, () => saveDishToFirestore(dish));
  });
  wrap.appendChild(cost);

  return wrap;
}

// ---------- Insumos page ----------
function renderInsumos() {
  const app = $('#app');
  app.innerHTML = '';
  const header = el('div', { class: 'insumos-header' },
    el('div', {},
      el('h1', {}, 'Insumos'),
      el('p', {}, isAdmin
        ? 'Atualize os preços — salva automaticamente e todas as fichas recalculam.'
        : 'Preços dos insumos. Faça login para editar.')
    ),
    el('input', { type: 'search', class: 'insumos-search', placeholder: 'Buscar insumo…' })
  );
  app.appendChild(header);

  const table = el('table', { class: 'insumos-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', {}, 'Unidade'),
      el('th', {}, 'Preço (R$)'),
      el('th', {}, 'Usado em'),
      isAdmin ? el('th', {}, '') : null
    ))
  );
  const tbody = el('tbody', {});

  const usageMap = {};
  DATA.dishes.forEach(d => {
    (d.sub_fichas || []).forEach(sf => {
      (sf.ingredientes || []).forEach(i => {
        usageMap[i.insumo_id] = (usageMap[i.insumo_id] || 0) + 1;
      });
    });
  });

  function buildRows(filter) {
    tbody.innerHTML = '';
    const f = (filter || '').toLowerCase().trim();
    DATA.insumos
      .filter(i => !f || i.name.toLowerCase().includes(f))
      .forEach(insumo => {
        const unitInput = el('input', { class: 'unit-input', value: insumo.unit || '', placeholder: 'g' });
        const priceInput = el('input', { type: 'number', min: '0', step: '0.01', value: insumo.price || 0 });
        if (!isAdmin) { unitInput.disabled = true; priceInput.disabled = true; }
        unitInput.addEventListener('change', () => {
          insumo.unit = unitInput.value.trim();
          scheduleSave('insumo-' + insumo.id, () => saveInsumoToFirestore(insumo));
        });
        priceInput.addEventListener('input', () => {
          insumo.price = parseFloat(priceInput.value) || 0;
          scheduleSave('insumo-' + insumo.id, () => saveInsumoToFirestore(insumo));
        });
        const cells = [
          el('td', { 'data-label': 'Insumo' }, insumo.name),
          el('td', { 'data-label': 'Unidade' }, unitInput),
          el('td', { 'data-label': 'Preço (R$)' }, priceInput),
          el('td', { 'data-label': 'Usado em' }, (usageMap[insumo.id] || 0) + ' receitas'),
        ];
        if (isAdmin) {
          const delBtn = el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
            const uses = usageMap[insumo.id] || 0;
            const msg = uses > 0
              ? `Excluir "${insumo.name}"? Ele é usado em ${uses} receita(s) — essas referências ficarão sem preço até você corrigir.`
              : `Excluir "${insumo.name}"?`;
            if (!confirm(msg)) return;
            try {
              await deleteInsumoFromFirestore(insumo.id);
              toast('Insumo excluído');
            } catch (err) { toast('Erro: ' + err.message); }
          } }, 'Excluir');
          cells.push(el('td', { 'data-label': '' }, delBtn));
        }
        const tr = el('tr', {}, ...cells);
        tbody.appendChild(tr);
      });
  }
  buildRows('');
  table.appendChild(tbody);
  app.appendChild(table);

  $('.insumos-search', header).addEventListener('input', e => buildRows(e.target.value));
}

// ---------- Admin: list ----------
function renderAdminList() {
  const app = $('#app');
  app.innerHTML = '';
  const header = el('div', { class: 'insumos-header' },
    el('div', {},
      el('h1', {}, 'Gerenciar fichas'),
      el('p', {}, 'Adicione, edite ou exclua pratos. Até 3 fotos por prato. Salva automaticamente no banco.')
    ),
    el('a', { class: 'btn btn-primary', href: '#/admin/new' }, '+ Nova ficha')
  );
  app.appendChild(header);

  const panel = el('div', { class: 'admin-panel' });
  const list = el('div', { class: 'dish-admin-list' });
  DATA.dishes.forEach(dish => {
    const item = el('div', { class: 'dish-admin-item' },
      el('div', { class: 'info' },
        el('h4', {}, dish.name),
        el('p', {}, `${(dish.sub_fichas || []).length} sub-fichas · ${dish.photos?.length || 0} fotos`)
      ),
      el('div', { class: 'dish-admin-actions' },
        el('a', { class: 'btn btn-small', href: `#/ficha/${dish.id}` }, 'Ver'),
        el('a', { class: 'btn btn-small btn-primary', href: `#/admin/edit/${dish.id}` }, 'Editar'),
        el('button', { class: 'btn btn-small btn-danger', onclick: () => deleteDishAction(dish.id) }, 'Excluir')
      )
    );
    list.appendChild(item);
  });
  panel.appendChild(list);
  app.appendChild(panel);
}

async function deleteDishAction(id) {
  const d = DATA.dishes.find(x => x.id === id);
  if (!d) return;
  if (!confirm(`Excluir "${d.name}" do banco? Essa ação não pode ser desfeita (mas você tem backup baixável).`)) return;
  try {
    await deleteDishFromFirestore(id);
    toast('Ficha excluída');
  } catch (err) { toast('Erro: ' + err.message); }
}

// ---------- Admin: edit/new ----------
function renderAdminEdit(dishId) {
  const app = $('#app');
  app.innerHTML = '';
  let dish;
  if (dishId) {
    const existing = DATA.dishes.find(d => d.id === dishId);
    if (!existing) { app.appendChild(el('p', {}, 'Prato não encontrado')); return; }
    dish = JSON.parse(JSON.stringify(existing));
  } else {
    dish = {
      id: 'new-' + uid(),
      name: '',
      description: '',
      photos: [],
      louca: '',
      equipamentos: [],
      sub_fichas: [{ id: uid(), name: 'Preparação 1', rendimento: '', ingredientes: [], modo_preparo: '' }],
      markup: 300,
    };
  }

  app.appendChild(el('a', { href: '#/admin', class: 'back-link' }, '← Voltar'));
  app.appendChild(el('h1', {}, dishId ? 'Editar ficha' : 'Nova ficha'));

  const panel = el('div', { class: 'admin-panel' });

  panel.appendChild(el('h2', {}, 'Informações gerais'));
  const basic = el('div', { class: 'form-grid' },
    fieldInput('Nome do prato', 'text', dish.name, v => dish.name = v),
    fieldInput('Louça / apresentação', 'text', dish.louca, v => dish.louca = v),
    fieldInput('Markup sugerido (%)', 'number', dish.markup, v => dish.markup = parseFloat(v) || 0)
  );
  panel.appendChild(basic);

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
          const file = input.files[0];
          if (!file) return;
          const reader = new FileReader();
          reader.onload = () => {
            const img = new Image();
            img.onload = () => {
              const maxW = 1000;
              const scale = Math.min(1, maxW / img.width);
              const canvas = document.createElement('canvas');
              canvas.width = img.width * scale;
              canvas.height = img.height * scale;
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
  (DATA.equipamentos_disponiveis || []).forEach(eq => {
    const lbl = el('label', {},
      (() => {
        const i = el('input', { type: 'checkbox' });
        if (dish.equipamentos.includes(eq)) i.setAttribute('checked', '');
        i.addEventListener('change', () => {
          if (i.checked) { if (!dish.equipamentos.includes(eq)) dish.equipamentos.push(eq); }
          else dish.equipamentos = dish.equipamentos.filter(x => x !== eq);
        });
        return i;
      })(),
      document.createTextNode(eq)
    );
    eqBox.appendChild(lbl);
  });
  panel.appendChild(eqBox);

  panel.appendChild(el('h2', {}, 'Sub-fichas (preparações)'));
  const sfWrap = el('div', {});
  function renderSubfichas() {
    sfWrap.innerHTML = '';
    dish.sub_fichas.forEach((sf, idx) => {
      const box = el('div', { class: 'subficha-editor' },
        el('h4', {},
          el('span', {}, `${idx + 1}. Preparação`),
          el('span', {},
            el('button', { class: 'btn btn-small', onclick: () => { if (idx > 0) { [dish.sub_fichas[idx-1], dish.sub_fichas[idx]] = [dish.sub_fichas[idx], dish.sub_fichas[idx-1]]; renderSubfichas(); } } }, '↑'),
            ' ',
            el('button', { class: 'btn btn-small', onclick: () => { if (idx < dish.sub_fichas.length - 1) { [dish.sub_fichas[idx], dish.sub_fichas[idx+1]] = [dish.sub_fichas[idx+1], dish.sub_fichas[idx]]; renderSubfichas(); } } }, '↓'),
            ' ',
            el('button', { class: 'btn btn-small btn-danger', onclick: () => { if (confirm('Excluir esta preparação?')) { dish.sub_fichas.splice(idx, 1); renderSubfichas(); } } }, 'Excluir')
          )
        ),
        el('div', { class: 'form-grid' },
          fieldInput('Nome da preparação', 'text', sf.name, v => sf.name = v),
          fieldInput('Rendimento', 'text', sf.rendimento, v => sf.rendimento = v)
        )
      );
      const ingHeader = el('h4', {}, 'Insumos');
      box.appendChild(ingHeader);
      const ingList = el('div', {});
      function renderIngs() {
        ingList.innerHTML = '';
        sf.ingredientes.forEach((ing, ingIdx) => {
          const row = el('div', { class: 'ingredient-row' });
          const nameInput = el('input', { type: 'text', value: ing.insumo_name, placeholder: 'Insumo', list: 'all-insumos' });
          nameInput.addEventListener('input', () => {
            ing.insumo_name = nameInput.value;
            const match = DATA.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
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
          row.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: () => { sf.ingredientes.splice(ingIdx, 1); renderIngs(); } }, '×'));
          ingList.appendChild(row);
        });
        const addBtn = el('button', { class: 'btn btn-small', onclick: () => {
          sf.ingredientes.push({ insumo_id: '', insumo_name: '', qty: null, qty_raw: '', unit: 'g', observacao: '', is_qb: false });
          renderIngs();
        } }, '+ adicionar insumo');
        ingList.appendChild(addBtn);
      }
      renderIngs();
      box.appendChild(ingList);
      box.appendChild(el('label', { class: 'field' },
        el('span', { class: 'label-text' }, 'Modo de Preparo'),
        (() => {
          const ta = el('textarea', {}, sf.modo_preparo || '');
          ta.addEventListener('input', () => { sf.modo_preparo = ta.value; });
          return ta;
        })()
      ));
      sfWrap.appendChild(box);
    });
    const add = el('button', { class: 'btn', onclick: () => {
      dish.sub_fichas.push({ id: uid(), name: `Preparação ${dish.sub_fichas.length + 1}`, rendimento: '', ingredientes: [], modo_preparo: '' });
      renderSubfichas();
    } }, '+ Adicionar sub-ficha');
    sfWrap.appendChild(add);
  }
  renderSubfichas();
  panel.appendChild(sfWrap);

  const dl = el('datalist', { id: 'all-insumos' });
  DATA.insumos.forEach(i => dl.appendChild(el('option', { value: i.name })));
  panel.appendChild(dl);

  const saveBar = el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn btn-primary', onclick: () => saveDish(dish, dishId) }, 'Salvar'),
    el('a', { class: 'btn', href: '#/admin' }, 'Cancelar')
  );
  panel.appendChild(saveBar);

  app.appendChild(panel);
}

async function saveDish(dish, originalId) {
  if (!isAdmin) { toast('Faça login como admin'); return; }
  if (!dish.name.trim()) { alert('Nome do prato é obrigatório'); return; }
  const newId = slugify(dish.name);
  dish.id = newId;
  try {
    // Register new insumos first
    const batch = writeBatch(db);
    let batchCount = 0;
    for (const sf of dish.sub_fichas) {
      for (const ing of sf.ingredientes) {
        if (!ing.insumo_name.trim()) continue;
        const existing = DATA.insumos.find(i => i.name.toLowerCase() === ing.insumo_name.toLowerCase());
        if (!existing) {
          const newIns = { id: slugify(ing.insumo_name), name: ing.insumo_name.trim(), unit: ing.unit || 'g', price: 0 };
          ing.insumo_id = newIns.id;
          const clean = { ...newIns };
          delete clean.id;
          batch.set(doc(db, 'insumos', newIns.id), clean);
          batchCount++;
        } else {
          ing.insumo_id = existing.id;
        }
      }
    }
    if (batchCount > 0) await batch.commit();
    // If id changed (on edit), delete old doc
    if (originalId && originalId !== newId) {
      await deleteDoc(doc(db, 'dishes', originalId));
    }
    await saveDishToFirestore(dish);
    toast('Ficha salva');
    location.hash = `#/ficha/${dish.id}`;
  } catch (err) {
    console.error(err);
    toast('Erro ao salvar: ' + err.message);
  }
}

function fieldInput(label, type, value, onChange) {
  const wrap = el('label', { class: 'field' }, el('span', { class: 'label-text' }, label));
  const input = el('input', { type, value: value ?? '' });
  input.addEventListener('input', () => onChange(input.value));
  wrap.appendChild(input);
  return wrap;
}

// ---------- Exports (PDF + XLSX) ----------
function exportFichaPDF(dish, state) {
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF({ unit: 'mm', format: 'a4' });
  const margin = 15;
  let y = margin;
  const pageWidth = doc.internal.pageSize.getWidth();
  const contentWidth = pageWidth - margin * 2;

  const isTrabalho = state.view === 'trabalho';
  const all = dishCost(dish);
  const currentSf = (dish.sub_fichas || []).find(s => s.id === state.activeSf) || (dish.sub_fichas || [])[0];

  doc.setFont('times', 'italic');
  doc.setFontSize(10);
  doc.setTextColor(130);
  doc.text(isTrabalho ? 'Ficha Técnica Operacional' : 'Ficha de Custo por Rendimento', pageWidth / 2, y, { align: 'center' });
  y += 7;
  doc.setFont('times', 'normal');
  doc.setFontSize(22);
  doc.setTextColor(20);
  doc.text(dish.name, pageWidth / 2, y, { align: 'center' });
  y += 10;
  doc.setDrawColor(184, 149, 94);
  doc.setLineWidth(0.3);
  doc.line(pageWidth / 2 - 20, y, pageWidth / 2 + 20, y);
  y += 8;

  if (isTrabalho) {
    doc.setFont('times', 'normal');
    doc.setFontSize(14);
    doc.setTextColor(20);
    doc.text(currentSf.name, margin, y);
    y += 5;
    if (currentSf.rendimento) {
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(9);
      doc.setTextColor(120);
      doc.text('Rendimento: ' + currentSf.rendimento, margin, y);
      y += 6;
    }
    doc.autoTable({
      startY: y,
      margin: { left: margin, right: margin },
      head: [['Insumo', 'Quantidade', 'Unidade', 'Observação']],
      body: (currentSf.ingredientes || []).map(i => [
        i.insumo_name,
        i.is_qb ? 'Q.B' : (i.qty != null ? fmtNum(i.qty, i.qty % 1 === 0 ? 0 : 2) : (i.qty_raw || '—')),
        i.unit || '—',
        i.observacao || '—'
      ]),
      theme: 'plain',
      headStyles: { fillColor: [247, 245, 238], textColor: [80, 80, 80], fontStyle: 'bold', fontSize: 8, cellPadding: 3 },
      bodyStyles: { fontSize: 9, cellPadding: 2.5, textColor: [40, 40, 40] },
      alternateRowStyles: { fillColor: [252, 251, 247] }
    });
    y = doc.lastAutoTable.finalY + 8;
    if (currentSf.modo_preparo) {
      if (y > 250) { doc.addPage(); y = margin; }
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(9);
      doc.setTextColor(100);
      doc.text('MODO DE PREPARO', margin, y);
      y += 5;
      doc.setFont('helvetica', 'normal');
      doc.setFontSize(9);
      doc.setTextColor(40);
      const lines = doc.splitTextToSize(currentSf.modo_preparo.replace(/\s+/g, ' '), contentWidth);
      doc.text(lines, margin, y);
      y += lines.length * 4 + 6;
    }
    if (dish.louca) {
      if (y > 260) { doc.addPage(); y = margin; }
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(9);
      doc.setTextColor(100);
      doc.text('APRESENTAÇÃO / LOUÇA', margin, y);
      y += 5;
      doc.setFont('helvetica', 'normal');
      doc.setTextColor(40);
      const lines = doc.splitTextToSize(dish.louca, contentWidth);
      doc.text(lines, margin, y);
      y += lines.length * 4 + 5;
    }
    if (dish.equipamentos && dish.equipamentos.length) {
      if (y > 260) { doc.addPage(); y = margin; }
      doc.setFont('helvetica', 'bold');
      doc.setFontSize(9);
      doc.setTextColor(100);
      doc.text('EQUIPAMENTOS', margin, y);
      y += 5;
      doc.setFont('helvetica', 'normal');
      doc.setTextColor(40);
      const lines = doc.splitTextToSize(dish.equipamentos.join(' · '), contentWidth);
      doc.text(lines, margin, y);
    }
  } else {
    all.sfCosts.forEach(({ sf, rows, total }) => {
      if (y > 240) { doc.addPage(); y = margin; }
      doc.setFont('times', 'normal');
      doc.setFontSize(12);
      doc.setTextColor(20);
      doc.text(`${sf.name}${sf.rendimento ? ' — ' + sf.rendimento : ''}`, margin, y);
      y += 4;
      doc.autoTable({
        startY: y,
        margin: { left: margin, right: margin },
        head: [['Insumo', 'Qtd', 'Un.', 'Preço unit.', 'Custo']],
        body: rows.map(({ ing, insumo, cost, isSubref, subSf }) => [
          (isSubref ? '↪ ' : '') + ing.insumo_name,
          ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—')),
          ing.unit || '—',
          isSubref ? `sub-ficha (${subSf?.rendimento || '—'})` : (insumo ? `${fmtBRL(insumo.price || 0)}/${insumo.unit || '—'}` : '—'),
          fmtBRL(cost)
        ]),
        foot: [['', '', '', 'Subtotal', fmtBRL(total)]],
        theme: 'plain',
        headStyles: { fillColor: [247, 245, 238], textColor: [80, 80, 80], fontStyle: 'bold', fontSize: 8, cellPadding: 2.5 },
        bodyStyles: { fontSize: 8.5, cellPadding: 2, textColor: [40, 40, 40] },
        footStyles: { fontStyle: 'bold', fillColor: [252, 251, 247], fontSize: 9 },
        columnStyles: { 1: { halign: 'right' }, 3: { halign: 'right' }, 4: { halign: 'right' } }
      });
      y = doc.lastAutoTable.finalY + 6;
    });
    if (y > 240) { doc.addPage(); y = margin; }
    doc.setFillColor(26, 26, 26);
    doc.rect(margin, y, contentWidth, 28, 'F');
    doc.setTextColor(255);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(8);
    const col = contentWidth / 3;
    doc.text('CUSTO TOTAL', margin + 5, y + 6);
    doc.text('CUSTO/PORÇÃO', margin + col + 5, y + 6);
    doc.text('PREÇO SUGERIDO', margin + col * 2 + 5, y + 6);
    doc.setFont('times', 'normal');
    doc.setFontSize(16);
    doc.text(fmtBRL(all.total), margin + 5, y + 18);
    doc.setTextColor(216, 184, 120);
    doc.text(fmtBRL(all.costPerPortion), margin + col + 5, y + 18);
    doc.setTextColor(255);
    doc.text(fmtBRL(all.suggestedPrice), margin + col * 2 + 5, y + 18);
    y += 32;

    doc.setTextColor(80);
    doc.setFont('helvetica', 'normal');
    doc.setFontSize(9);
    doc.text(`Markup: ${dish.markup}%   ·   CMV: ${fmtNum(all.cmv, 1)}%   ·   Porções: ${all.portions}`, margin, y);
  }

  doc.save(`${slugify(dish.name)}-${isTrabalho ? 'trabalho' : 'custo'}-${currentSf?.id || 'geral'}.pdf`);
}

function exportFichaXLSX(dish) {
  const wb = XLSX.utils.book_new();
  const all = dishCost(dish);
  const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
  const summaryData = [
    ['FICHA TÉCNICA'],
    ['Prato', dish.name],
    ['Rendimento final', lastSf?.rendimento || '—'],
    [],
    ['Custo total', all.total],
    ['Porções', all.portions],
    ['Custo por porção', all.costPerPortion],
    ['Markup (%)', dish.markup],
    ['Preço de venda sugerido', all.suggestedPrice],
    ['CMV (%)', all.cmv],
    [],
    ['Louça', dish.louca || '—'],
    ['Equipamentos', (dish.equipamentos || []).join('; ')],
  ];
  const wsSum = XLSX.utils.aoa_to_sheet(summaryData);
  XLSX.utils.book_append_sheet(wb, wsSum, 'Resumo');
  all.sfCosts.forEach(({ sf, rows, total }, idx) => {
    const data = [
      [sf.name],
      ['Rendimento', sf.rendimento || '—'],
      [],
      ['Insumo', 'Quantidade', 'Unidade', 'Observação', 'Preço unit.', 'Custo'],
      ...rows.map(({ ing, insumo, cost }) => [
        ing.insumo_name,
        ing.is_qb ? 'Q.B' : (ing.qty != null ? ing.qty : (ing.qty_raw || '')),
        ing.unit || '',
        ing.observacao || '',
        insumo ? (insumo.price || 0) : 0,
        cost
      ]),
      [],
      ['Subtotal', '', '', '', '', total],
      [],
      ['Modo de preparo:'],
      [sf.modo_preparo || '']
    ];
    const ws = XLSX.utils.aoa_to_sheet(data);
    const name = ('SF' + (idx + 1) + '-' + sf.name).replace(/[/\\?*\[\]:]/g, '').slice(0, 30);
    XLSX.utils.book_append_sheet(wb, ws, name);
  });
  XLSX.writeFile(wb, `${slugify(dish.name)}.xlsx`);
}

// ---------- Init ----------
onAuthStateChanged(auth, (user) => {
  currentUser = user;
  isAdmin = !!(user && user.email === ADMIN_EMAIL);
  updateAuthUI();
  if (hasRenderedOnce) route();
});

setupListeners();

// Wire up buttons
$('#btn-auth').addEventListener('click', () => {
  if (isAdmin) doLogout();
  else openLoginModal();
});
$('#login-form').addEventListener('submit', e => {
  e.preventDefault();
  doLogin($('#login-email').value, $('#login-password').value);
});
$('#login-cancel').addEventListener('click', closeLoginModal);
$('.modal-overlay').addEventListener('click', closeLoginModal);
$('#btn-download-backup').addEventListener('click', downloadBackup);
$('#btn-seed').addEventListener('click', seedFromJson);
$('#file-restore').addEventListener('change', e => {
  if (e.target.files[0]) restoreBackup(e.target.files[0]);
});

// Initial UI
updateAuthUI();
