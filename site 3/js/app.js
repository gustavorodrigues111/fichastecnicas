/* ================================================================
   Fichas Técnicas — multi-tenant SPA (Firebase + vanilla JS)
   ================================================================ */

import { initializeApp, getApps } from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-app.js';
import {
  getFirestore, collection, doc, setDoc, deleteDoc, onSnapshot, addDoc,
  getDoc, getDocs, writeBatch, collectionGroup, serverTimestamp, query, where,
  deleteField
} from 'https://www.gstatic.com/firebasejs/10.12.0/firebase-firestore.js';
import {
  getAuth, signInWithEmailAndPassword, createUserWithEmailAndPassword,
  sendPasswordResetEmail, onAuthStateChanged, signOut, updatePassword
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
// Escolhe melhor unidade pra exibir peso: kg se >= 1kg, g se < 1kg (3 casas pra subgramas)
function pickWeightDisplay(qty_kg) {
  if (qty_kg >= 1) return { qty: qty_kg, unit: 'kg', text: fmtNum(qty_kg, 3) };
  // abaixo de 1 kg → mostra em gramas
  const g = qty_kg * 1000;
  const dec = g >= 10 ? 1 : (g >= 1 ? 2 : 3);
  return { qty: g, unit: 'g', text: fmtNum(g, dec) };
}
// Idem para volume
function pickVolumeDisplay(qty_l) {
  if (qty_l >= 1) return { qty: qty_l, unit: 'l', text: fmtNum(qty_l, 3) };
  const ml = qty_l * 1000;
  const dec = ml >= 10 ? 1 : (ml >= 1 ? 2 : 3);
  return { qty: ml, unit: 'ml', text: fmtNum(ml, dec) };
}
function normUnitForDisplay(qty, unit) {
  if (qty == null) return { qty: null, unit: unit || '', text: '—' };
  const u = (unit || '').toLowerCase().trim().replace(/\s+/g, '');
  if (WEIGHT_G.includes(u)) return pickWeightDisplay(qty / 1000);
  if (WEIGHT_MG.includes(u)) return pickWeightDisplay(qty / 1e6);
  if (WEIGHT_KG.includes(u)) return pickWeightDisplay(qty);
  if (VOL_ML.includes(u)) return pickVolumeDisplay(qty / 1000);
  if (VOL_L.includes(u)) return pickVolumeDisplay(qty);
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
// Roles do sistema:
//   master         — Gustavo. Cria/edita tudo. Único que cria clientes e Minha Equipe (staff).
//   staff          — "Minha Equipe" (consultoria). Cria/edita fichas e preços nos restaurantes atribuídos.
//   cliente        — Dono do restaurante. Vê tudo, edita preços, planeja produção, cria sua equipe.
//                    Cria fichas próprias (desbloqueadas). Não edita fichas locked (criadas pela consultoria).
//   cliente_admin  — Equipe administrativa do cliente. Vê preços, planeja produção. Não cria fichas.
//   cliente_op     — Equipe operacional do cliente. Acessa fichas e planeja produção. Não vê preços.
//   equipe (legado)— Mapeado dinamicamente pra cliente_admin (se via insumos) ou cliente_op.
const isMaster = () => STATE.userDoc && (STATE.userDoc.role === 'master' || STATE.user?.email === MASTER_EMAIL);
const isStaff = () => STATE.userDoc && STATE.userDoc.role === 'staff';
const isClienteUser = () => STATE.userDoc && STATE.userDoc.role === 'cliente';
const isClienteAdmin = () => STATE.userDoc && (STATE.userDoc.role === 'cliente_admin'
  || (STATE.userDoc.role === 'equipe' && !!STATE.userDoc.permissions?.can_view_insumos));
const isClienteOp = () => STATE.userDoc && (STATE.userDoc.role === 'cliente_op'
  || (STATE.userDoc.role === 'equipe' && !STATE.userDoc.permissions?.can_view_insumos));
// Compatibilidade: qualquer perfil de cliente (dono ou equipe)
const isAnyClienteSide = () => isClienteUser() || isClienteAdmin() || isClienteOp();

const hasClientAccess = (cid) => isMaster() || (STATE.userDoc?.clienteIds || []).includes(cid);
const canViewCliente = (cid) => isMaster() || (STATE.userDoc?.clienteIds || []).includes(cid);

// Edição de fichas (admin completo do restaurante): só master + staff
const canEditCliente = (cid) => isMaster() || (isStaff() && (STATE.userDoc.clienteIds || []).includes(cid));

// Edição de preço de insumo: master, staff, cliente (dono), cliente_admin
const canEditInsumoPrice = (cid) => canEditCliente(cid)
  || ((isClienteUser() || isClienteAdmin()) && (STATE.userDoc.clienteIds || []).includes(cid));

// Visualização das abas
const canViewCardapio = (cid) => canViewCliente(cid); // todos veem o cardápio
const canViewInsumosTab = (cid) => canViewCliente(cid) && (isMaster() || isStaff() || isClienteUser() || isClienteAdmin());
const canViewProducao = (cid) => canViewCliente(cid); // todos veem produção

// Cliente (dono) gerencia equipe do próprio restaurante
const canManageClientTeam = (cid) => isMaster() || (isClienteUser() && (STATE.userDoc.clienteIds || []).includes(cid));

// Edição de uma ficha específica (considera lock):
//   - Ficha locked (default pra fichas criadas por master/staff): só master e staff editam
//   - Ficha unlocked: master, staff e cliente (dono) editam
//   - cliente_admin, cliente_op: nunca editam fichas (só veem)
function canEditDish(cid, dish) {
  if (!canViewCliente(cid)) return false;
  if (isMaster() || (isStaff() && hasClientAccess(cid))) return true;
  if (isClienteUser() && hasClientAccess(cid)) {
    // Cliente pode editar se a ficha NÃO está locked
    return !dish.locked;
  }
  return false;
}
function canCreateDish(cid) {
  if (isMaster()) return true;
  if (isStaff() && hasClientAccess(cid)) return true;
  if (isClienteUser() && hasClientAccess(cid)) return true; // cliente cria fichas próprias
  return false;
}
function canToggleDishLock(cid) {
  // Só master e staff travam/destravam fichas
  return isMaster() || (isStaff() && hasClientAccess(cid));
}

const canManageUsers = () => isMaster();
const canManageClientes = () => isMaster();

const ROLE_LABELS = {
  master: 'Master',
  staff: 'Minha Equipe',
  cliente: 'Cliente (dono)',
  cliente_admin: 'Equipe Admin do Cliente',
  cliente_op: 'Equipe Operacional',
  equipe: 'Equipe (legado)'
};

// ---------- Trial / assinatura ----------
const TRIAL_DEFAULT_DAYS = 180; // 6 meses
function getTrialStatus(cliente) {
  if (!cliente) return { paid: false, started: false, isBlocked: false };
  if (cliente.subscription_active === true) {
    return { paid: true, started: true, isBlocked: false };
  }
  if (!cliente.trial_started_at) {
    return { paid: false, started: false, isBlocked: true };
  }
  const startedMs = new Date(cliente.trial_started_at).getTime();
  if (isNaN(startedMs)) return { paid: false, started: false, isBlocked: true };
  const days = (typeof cliente.trial_days === 'number' && cliente.trial_days > 0) ? cliente.trial_days : TRIAL_DEFAULT_DAYS;
  const endsMs = startedMs + days * 24 * 60 * 60 * 1000;
  const now = Date.now();
  const daysLeft = Math.ceil((endsMs - now) / (24 * 60 * 60 * 1000));
  return {
    paid: false,
    started: true,
    isBlocked: now > endsMs,
    daysLeft,
    daysTotal: days,
    startedAt: cliente.trial_started_at,
    endsAt: new Date(endsMs).toISOString()
  };
}
function trialGate(cid) {
  if (isMaster() || isStaff()) return false;
  const cliente = STATE.currentCliente;
  if (!cliente || cliente.id !== cid) return false;
  const status = getTrialStatus(cliente);
  if (status.paid) return false;
  if (!status.started) { renderTrialNotStarted(cliente); return true; }
  if (status.isBlocked) { renderTrialExpired(cliente, status); return true; }
  return false;
}
function renderTrialBanner(cid) {
  const cliente = STATE.currentCliente;
  if (!cliente || cliente.id !== cid) return null;
  const status = getTrialStatus(cliente);
  if (status.paid) return null;
  if (!status.started) {
    if (!(isMaster() || isStaff())) return null;
    return el('div', { class: 'trial-banner trial-banner-warning' },
      'Avaliação não iniciada para este restaurante. ',
      el('a', { href: `#/c/${cid}/admin` }, 'Configurar →')
    );
  }
  if (status.daysLeft > 30) return null;
  const isCritical = status.daysLeft <= 1;
  const isUrgent = status.daysLeft <= 7;
  const cls = 'trial-banner ' + (isCritical ? 'trial-banner-critical' : (isUrgent ? 'trial-banner-urgent' : 'trial-banner-warning'));
  let msg;
  if (status.daysLeft <= 0) msg = 'Avaliação termina hoje';
  else if (status.daysLeft === 1) msg = 'Avaliação termina amanhã';
  else msg = `Faltam ${status.daysLeft} dias na avaliação`;
  return el('div', { class: cls }, msg);
}
function renderTrialNotStarted(cliente) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('section', { class: 'trial-block' },
    el('div', { class: 'trial-block-card' },
      el('h1', {}, 'Acesso ainda não liberado'),
      el('p', {}, 'O período de avaliação deste restaurante ainda não foi iniciado.'),
      cliente.consultor_name ? el('p', { class: 'muted' }, 'Entre em contato com ',
        el('strong', {}, cliente.consultor_name),
        cliente.consultor_info ? ' · ' + cliente.consultor_info : ''
      ) : null,
      el('button', { class: 'btn', onclick: doLogout }, 'Sair')
    )
  ));
}
function renderTrialExpired(cliente, status) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('section', { class: 'trial-block' },
    el('div', { class: 'trial-block-card' },
      el('h1', {}, 'Avaliação encerrada'),
      el('p', {}, `Sua avaliação gratuita de ${status.daysTotal} dias chegou ao fim.`),
      el('p', {}, 'Para continuar usando o sistema, entre em contato para ativar sua assinatura.'),
      cliente.consultor_name ? el('p', { class: 'consultor-line' }, el('strong', {}, cliente.consultor_name),
        cliente.consultor_info ? ' · ' + cliente.consultor_info : ''
      ) : null,
      el('button', { class: 'btn', onclick: doLogout }, 'Sair')
    )
  ));
}

// ---------- Firestore paths ----------
const dishesCol = (cid) => collection(db, 'clientes', cid, 'dishes');
const dishDoc = (cid, did) => doc(db, 'clientes', cid, 'dishes', did);
const insumosCol = (cid) => collection(db, 'clientes', cid, 'insumos');
const insumoDoc = (cid, iid) => doc(db, 'clientes', cid, 'insumos', iid);
const configDoc = (cid, name) => doc(db, 'clientes', cid, 'config', name);
const clienteDoc = (cid) => doc(db, 'clientes', cid);
// Stock module
const stockTemplatesCol = (cid) => collection(db, 'clientes', cid, 'stock_templates');
const stockTemplateDoc = (cid, tid) => doc(db, 'clientes', cid, 'stock_templates', tid);
const stockCountsCol = (cid) => collection(db, 'clientes', cid, 'stock_counts');
const stockCountDoc = (cid, countId) => doc(db, 'clientes', cid, 'stock_counts', countId);
const stockCountLogsCol = (cid, countId) => collection(db, 'clientes', cid, 'stock_counts', countId, 'logs');

// ---------- Firestore writes ----------
async function saveDish(cid, dish) {
  const clean = { ...dish };
  delete clean.id;
  await setDoc(dishDoc(cid, dish.id), clean);
}
async function deleteDish(cid, did) {
  await deleteDoc(dishDoc(cid, did));
  // Auto-cleanup: remove insumos órfãos (que ficaram sem nenhuma ficha referenciando)
  await cleanupOrphanInsumos(cid).catch(err => console.warn('cleanup insumos:', err));
}

// Remove insumos que não são referenciados por nenhuma ficha, OU cujo nome bate com um subproduto/subref
async function cleanupOrphanInsumos(cid) {
  const [insumosSnap, dishesSnap] = await Promise.all([
    getDocs(insumosCol(cid)),
    getDocs(dishesCol(cid))
  ]);
  // Coleta nomes de subprodutos (não são insumos — são produzidos)
  const subprodutoNames = new Set();
  // Coleta nomes de sub-fichas (subrefs por detecção)
  const sfNames = new Set();
  // IDs de insumos referenciados por qualquer ingrediente real (não subref)
  const referenced = new Set();
  // Nomes de insumos que são referenciados por nome (mas podem não ter insumo_id setado ainda)
  const referencedByName = new Set();
  dishesSnap.docs.forEach(d => {
    const data = d.data();
    (data.sub_fichas || []).forEach(sf => {
      if (sf.name) sfNames.add(nrm(sf.name));
      if (sf.subproduto && sf.subproduto.name) subprodutoNames.add(nrm(sf.subproduto.name));
      (sf.ingredientes || []).forEach(ing => {
        if (!ing.subref_id && ing.insumo_id) referenced.add(ing.insumo_id);
        if (!ing.subref_id && ing.insumo_name) referencedByName.add(nrm(ing.insumo_name));
      });
    });
  });
  // Deletar insumos: não referenciados OR cujo nome bate com subproduto/sub-ficha
  let batch = writeBatch(db);
  let count = 0, deleted = 0;
  for (const ins of insumosSnap.docs) {
    const data = ins.data();
    const n = nrm(data.name || '');
    const isSubproduto = subprodutoNames.has(n);
    const isSfName = sfNames.has(n);
    const isOrphan = !referenced.has(ins.id) && !referencedByName.has(n);
    if (isOrphan || isSubproduto || isSfName) {
      batch.delete(insumoDoc(cid, ins.id));
      deleted++;
      count++;
      if (count >= 400) { await batch.commit(); batch = writeBatch(db); count = 0; }
    }
  }
  if (count > 0) await batch.commit();
  return deleted;
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
  STATE.currentClienteId = null;
  STATE.loaded = false;
  updateBreadcrumb();
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
    updateBreadcrumb();
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
    // Atualiza editor de equipamentos aberto, se houver
    const eqBox = document.querySelector('.equipamentos-select');
    if (eqBox && eqBox.__rerender) eqBox.__rerender();
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

// ---------- Modal de editar cliente (nome, consultor, toggle) ----------
function openEditClienteModal(cliente) {
  const existing = $('#edit-cliente-modal');
  if (existing) existing.remove();

  const nameInput = el('input', { type: 'text', value: cliente.name || '' });
  const consultorNameInput = el('input', { type: 'text', value: cliente.consultor_name || '', placeholder: 'Ex: Gustavo Rodrigues' });
  const consultorInfoInput = el('input', { type: 'text', value: cliente.consultor_info || '', placeholder: 'Ex: Consultoria Gastronômica · contato@quibebe.com.br' });
  const showToggle = el('input', { type: 'checkbox' });
  if (cliente.show_consultor !== false) showToggle.setAttribute('checked', ''); // default true

  const modal = el('div', { class: 'modal', id: 'edit-cliente-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content modal-content-wide' },
      el('h2', {}, 'Editar restaurante'),
      el('p', { class: 'modal-subtitle' }, cliente.id),
      el('div', { class: 'form-grid' },
        el('label', { class: 'field field-wide' },
          el('span', { class: 'label-text' }, 'Nome do restaurante'), nameInput)
      ),
      el('h3', { style: 'margin-top:1.5rem;margin-bottom:0.5rem;font-size:0.78rem;text-transform:uppercase;letter-spacing:0.14em;color:#888;font-family:Inter,sans-serif;font-weight:500;' }, 'Consultoria'),
      el('p', { class: 'muted', style: 'font-size:0.85rem;margin-bottom:1rem;' }, 'Aparece discretamente no cardápio, nas fichas e no rodapé dos PDFs exportados.'),
      el('div', { class: 'form-grid' },
        el('label', { class: 'field' },
          el('span', { class: 'label-text' }, 'Nome do consultor'), consultorNameInput),
        el('label', { class: 'field' },
          el('span', { class: 'label-text' }, 'Info adicional'), consultorInfoInput)
      ),
      el('label', { class: 'field', style: 'display:flex;flex-direction:row;align-items:center;gap:0.6rem;margin-top:0.75rem;' },
        showToggle,
        el('span', { style: 'font-size:0.9rem;color:#4a4a4a;' }, 'Exibir info da consultoria no site e exports')
      ),
      buildTrialBlock(cliente),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: async () => {
          const trialBlock = modal.querySelector('.trial-block-fields');
          const trialValues = trialBlock && typeof trialBlock.__getValues === 'function' ? trialBlock.__getValues() : {};
          const updated = {
            ...cliente,
            name: nameInput.value.trim() || cliente.name,
            consultor_name: consultorNameInput.value.trim(),
            consultor_info: consultorInfoInput.value.trim(),
            show_consultor: showToggle.checked,
            ...trialValues
          };
          try {
            await saveCliente(updated);
            toast('Restaurante atualizado');
            modal.remove();
            renderClientesAdmin();
          } catch (err) { toast('Erro: ' + err.message); }
        } }, 'Salvar')
      )
    )
  );
  document.body.appendChild(modal);
}

// Bloco editável de trial/assinatura no modal de cliente (master)
function buildTrialBlock(cliente) {
  const wrap = el('div', { class: 'trial-block-fields' });
  wrap.appendChild(el('h3', { style: 'margin-top:1.5rem;margin-bottom:0.5rem;font-size:0.78rem;text-transform:uppercase;letter-spacing:0.14em;color:#888;font-family:Inter,sans-serif;font-weight:500;' }, 'Assinatura'));
  const status = getTrialStatus(cliente);
  let statusLine;
  if (status.paid) statusLine = el('span', { style: 'color:#177c4a;font-weight:600;' }, '● Assinatura ativa (sem expiração)');
  else if (!status.started) statusLine = el('span', { style: 'color:#888;' }, '○ Avaliação não iniciada');
  else if (status.isBlocked) statusLine = el('span', { style: 'color:#b1272e;font-weight:600;' }, `✕ Avaliação expirada em ${new Date(status.endsAt).toLocaleDateString('pt-BR')}`);
  else statusLine = el('span', { style: 'color:#8a6b40;font-weight:600;' }, `● Em avaliação · ${status.daysLeft} dia(s) restantes (até ${new Date(status.endsAt).toLocaleDateString('pt-BR')})`);
  wrap.appendChild(el('p', { class: 'muted', style: 'font-size:0.9rem;margin-bottom:0.75rem;' }, statusLine));

  const daysInput = el('input', { type: 'number', min: '1', step: '1', value: (typeof cliente.trial_days === 'number' && cliente.trial_days > 0) ? cliente.trial_days : TRIAL_DEFAULT_DAYS });
  const subToggle = el('input', { type: 'checkbox' });
  if (cliente.subscription_active === true) subToggle.setAttribute('checked', '');
  const startedInput = el('input', { type: 'date', value: cliente.trial_started_at ? new Date(cliente.trial_started_at).toISOString().slice(0, 10) : '' });

  wrap.appendChild(el('div', { class: 'form-grid' },
    el('label', { class: 'field' },
      el('span', { class: 'label-text' }, 'Duração da avaliação (dias)'), daysInput),
    el('label', { class: 'field' },
      el('span', { class: 'label-text' }, 'Avaliação iniciada em'), startedInput)
  ));
  wrap.appendChild(el('div', { style: 'display:flex;gap:0.5rem;margin-top:0.5rem;flex-wrap:wrap;' },
    el('button', { class: 'btn btn-small', onclick: (e) => { e.preventDefault(); startedInput.value = new Date().toISOString().slice(0, 10); } }, 'Iniciar hoje'),
    el('button', { class: 'btn btn-small btn-danger', onclick: (e) => { e.preventDefault(); if (confirm('Resetar avaliação? O cliente será bloqueado até você reiniciar.')) { startedInput.value = ''; subToggle.checked = false; } } }, 'Resetar')
  ));
  wrap.appendChild(el('label', { class: 'field', style: 'display:flex;flex-direction:row;align-items:center;gap:0.6rem;margin-top:0.75rem;' },
    subToggle,
    el('span', { style: 'font-size:0.9rem;color:#4a4a4a;' }, 'Assinatura ativa (pago — libera sem expiração)')
  ));
  wrap.__getValues = () => ({
    trial_days: parseInt(daysInput.value, 10) || TRIAL_DEFAULT_DAYS,
    trial_started_at: startedInput.value ? new Date(startedInput.value + 'T12:00:00').toISOString() : null,
    subscription_active: subToggle.checked
  });
  return wrap;
}

// ---------- Modal unificado para escolher tipo de atualização ----------
function openUpdateFromFileModal(cliente) {
  const existing = $('#update-from-file-modal');
  if (existing) existing.remove();

  const modal = el('div', { class: 'modal', id: 'update-from-file-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content modal-content-wide' },
      el('h2', {}, `Atualizar "${cliente.name}" a partir do arquivo`),
      el('p', { class: 'modal-subtitle' }, 'Escolha o que sincronizar do arquivo data.json'),
      (() => {
        const form = el('form', { class: 'update-options-form' });
        const opts = [
          { val: 'prices', label: 'Só preços', icon: '₴', tag: 'SEGURO',
            desc: 'Atualiza só o preço dos insumos. Não toca em fichas, nem adiciona ou remove insumos. Preço existente é sobrescrito pelo do arquivo se o arquivo tiver preço.' },
          { val: 'structure', label: 'Só estrutura', icon: '⚙', tag: 'SEGURO',
            desc: 'Atualiza rendimento (qty + unidade), subref_id (referências entre sub-fichas) e fator de correção. Preserva tudo que você editou (nomes, modo de preparo, fotos, preços, markup/CMV, observações).' },
          { val: 'full', label: 'Tudo', icon: '⟳', tag: '⚠ SOBRESCREVE',
            desc: 'Reimporta completo. Sobrescreve fichas com a versão do arquivo, preserva só preços de insumos > 0. Insumos e fichas órfãos (que não estão no arquivo) são excluídos. Use se você não editou nada localmente ou se quer resetar tudo.' }
        ];
        opts.forEach((o, i) => {
          const id = 'upd-opt-' + o.val;
          const card = el('label', { class: 'upd-opt', for: id },
            el('input', { type: 'radio', name: 'upd-type', value: o.val, id, ...(i === 1 ? { checked: '' } : {}) }),
            el('div', { class: 'upd-opt-body' },
              el('div', { class: 'upd-opt-head' },
                el('span', { class: 'upd-opt-icon' }, o.icon),
                el('span', { class: 'upd-opt-label' }, o.label),
                el('span', { class: 'upd-opt-tag' + (o.tag.startsWith('⚠') ? ' warn' : '') }, o.tag)
              ),
              el('p', { class: 'upd-opt-desc' }, o.desc)
            )
          );
          form.appendChild(card);
        });
        return form;
      })(),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: async () => {
          const selected = $('input[name="upd-type"]:checked', modal)?.value;
          if (!selected) return;
          modal.remove();
          if (selected === 'prices') syncPricesOnly(cliente.id);
          else if (selected === 'structure') syncStructureOnly(cliente.id);
          else if (selected === 'full') {
            if (!confirm(`Reimportar TUDO em "${cliente.name}"?\n\nSobrescreve fichas e pode perder edições que você fez no admin.\nPreços existentes > 0 são preservados.`)) return;
            seedClienteFromJson(cliente.id, cliente.name).catch(e => toast('Erro: ' + e.message));
          }
        } }, 'Atualizar')
      )
    )
  );
  document.body.appendChild(modal);
}

// ---------- Sync estrutural: só correções de estrutura, preserva conteúdo editado ----------
async function syncStructureOnly(cid) {
  if (!isMaster() && !isStaff()) { toast('Sem permissão'); return; }
  const ok = confirm(
    'Sincronizar correções estruturais?\n\n' +
    '• ATUALIZA: rendimento (qty + unidade) das sub-fichas, subref_id (referências entre sub-fichas), fc (fator de correção) dos ingredientes.\n\n' +
    '• PRESERVA: nomes, modo de preparo, fotos, preços, markup/CMV, observações, louça, equipamentos — tudo o que você editou no admin.\n\n' +
    'Fichas novas do data.json que não existem no seu cliente serão adicionadas. Fichas que você tem mas não estão no data.json serão mantidas.'
  );
  if (!ok) return;
  try {
    toast('Sincronizando estrutura...');
    const resp = await fetch('data/data.json');
    const seed = await resp.json();
    const existingSnap = await getDocs(dishesCol(cid));
    const existingMap = {};
    existingSnap.docs.forEach(d => { existingMap[d.id] = { id: d.id, ...d.data() }; });

    let batch = writeBatch(db);
    let count = 0;
    let updatedDishes = 0, addedDishes = 0;
    const flush = async () => { if (count > 0) { await batch.commit(); batch = writeBatch(db); count = 0; } };

    for (const seedDish of seed.dishes) {
      const existing = existingMap[seedDish.id];
      if (!existing) {
        // Ficha não existe → adiciona integral
        const clean = { ...seedDish };
        delete clean.id;
        batch.set(dishDoc(cid, seedDish.id), clean);
        count++; addedDishes++;
        if (count >= 400) await flush();
        continue;
      }

      // Merge estrutural
      let changed = false;
      const existingSubs = [...(existing.sub_fichas || [])];

      for (const seedSf of seedDish.sub_fichas || []) {
        // Match por id, senão por nome
        let existingSf = existingSubs.find(s => s.id === seedSf.id) ||
                         existingSubs.find(s => (s.name||'').toLowerCase() === (seedSf.name||'').toLowerCase());
        if (!existingSf) {
          // Sub-ficha nova no seed → adiciona
          existingSubs.push(JSON.parse(JSON.stringify(seedSf)));
          changed = true;
          continue;
        }
        // Atualiza só rendimento_qty e rendimento_unit
        if (seedSf.rendimento_qty != null && existingSf.rendimento_qty !== seedSf.rendimento_qty) {
          existingSf.rendimento_qty = seedSf.rendimento_qty; changed = true;
        }
        if (seedSf.rendimento_unit && existingSf.rendimento_unit !== seedSf.rendimento_unit) {
          existingSf.rendimento_unit = seedSf.rendimento_unit; changed = true;
        }
        // Atualiza ingredientes: só subref_id e fc, matching por insumo_id ou nome
        for (const seedIng of seedSf.ingredientes || []) {
          const existingIngs = existingSf.ingredientes || [];
          let existingIng = existingIngs.find(i => i.insumo_id && i.insumo_id === seedIng.insumo_id) ||
                            existingIngs.find(i => (i.insumo_name||'').toLowerCase() === (seedIng.insumo_name||'').toLowerCase());
          if (!existingIng) continue;
          // subref_id
          if (seedIng.subref_id && existingIng.subref_id !== seedIng.subref_id) {
            existingIng.subref_id = seedIng.subref_id; changed = true;
          } else if (!seedIng.subref_id && existingIng.subref_id) {
            delete existingIng.subref_id; changed = true;
          }
          // fc
          if (seedIng.fc != null && existingIng.fc !== seedIng.fc) {
            existingIng.fc = seedIng.fc; changed = true;
          } else if (seedIng.fc == null && existingIng.fc) {
            delete existingIng.fc; changed = true;
          }
        }
      }

      if (changed) {
        existing.sub_fichas = existingSubs;
        const clean = { ...existing };
        delete clean.id;
        batch.set(dishDoc(cid, seedDish.id), clean);
        count++; updatedDishes++;
        if (count >= 400) await flush();
      }
    }
    await flush();
    toast(`✓ Estrutura sincronizada: ${updatedDishes} fichas atualizadas${addedDishes ? `, ${addedDishes} adicionadas` : ''}`);
  } catch (err) {
    console.error(err);
    toast('Erro ao sincronizar: ' + err.message);
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

// ---------- Excel import/export ----------
// Gera planilha modelo com 4 abas: Insumos, Variações, Fichas, Equipamentos
function downloadExcelTemplate() {
  const XLSX = window.XLSX;
  const wb = XLSX.utils.book_new();

  const insumosData = [
    ['nome', 'unidade', 'preco', 'reutilizavel'],
    ['Cebola Branca', 'kg', 4.50, ''],
    ['Sal Refinado', 'kg', 3.20, ''],
    ['Azeite', 'l', 28.00, ''],
    ['Óleo de Fritura', 'l', 9.00, 'sim'],
  ];
  const variacoesData = [
    ['insumo_base', 'variacao', 'fc'],
    ['Cebola Branca', 'brunoise', 1.15],
    ['Cebola Branca', 'julienne', 1.12],
  ];
  const fichasData = [
    ['ficha', 'sub_ficha', 'rendimento_qty', 'rendimento_unit', 'insumo', 'variacao', 'qtd', 'unidade', 'fc', 'observacao', 'modo_preparo', 'subref'],
    ['Arroz com Feijão', 'Feijão', 6, 'kg', 'Feijão Preto', '', 1, 'kg', '', 'demolhado', 'Cozinhar o feijão em panela de pressão por 30min', ''],
    ['Arroz com Feijão', 'Feijão', '', '', 'Cebola Branca', 'brunoise', 0.05, 'kg', '', '', '', ''],
    ['Arroz com Feijão', 'Montagem', 1, 'porção', 'Feijão', '', 0.2, 'kg', '', '', 'Sirva quente', 'Feijão'],
  ];
  const equipamentosData = [['nome'], ['Fogão'], ['Forno'], ['Liquidificador']];

  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(insumosData), 'Insumos');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(variacoesData), 'Variacoes');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(fichasData), 'Fichas');
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(equipamentosData), 'Equipamentos');

  XLSX.writeFile(wb, 'modelo-importacao-appmise.xlsx');
}

// Lê arquivo Excel, extrai estrutura de dados parseada
async function parseExcelFile(file) {
  const XLSX = window.XLSX;
  const buf = await file.arrayBuffer();
  const wb = XLSX.read(buf, { type: 'array' });
  const readSheet = (name) => {
    const ws = wb.Sheets[name];
    if (!ws) return [];
    return XLSX.utils.sheet_to_json(ws, { defval: '' });
  };
  const rawInsumos = readSheet('Insumos');
  const rawVariacoes = readSheet('Variacoes');
  const rawFichas = readSheet('Fichas');
  const rawEquipamentos = readSheet('Equipamentos');

  // Monta insumos (map por nome lowercase)
  const insumosByName = {};
  for (const r of rawInsumos) {
    const name = String(r.nome || '').trim();
    if (!name) continue;
    const unit = String(r.unidade || 'kg').trim().toLowerCase();
    const price = parseFloat(r.preco) || 0;
    const id = slugify(name);
    const reut = String(r.reutilizavel || '').trim().toLowerCase();
    const isReut = reut === 'sim' || reut === 's' || reut === '1' || reut === 'true' || reut === 'x';
    const payload = { id, name, unit, price };
    if (isReut) payload.reutilizavel = true;
    insumosByName[name.toLowerCase()] = payload;
  }
  // Variações
  for (const r of rawVariacoes) {
    const base = String(r.insumo_base || '').trim().toLowerCase();
    const varName = String(r.variacao || '').trim();
    const fc = parseFloat(r.fc);
    if (!base || !varName || !fc) continue;
    const ins = insumosByName[base];
    if (!ins) continue;
    if (!ins.variations) ins.variations = [];
    ins.variations.push({ name: varName, fc });
  }
  // Fichas: agrupa por ficha → sub-ficha → lista de ingredientes
  const dishes = {};
  for (const r of rawFichas) {
    const dishName = String(r.ficha || '').trim();
    const sfName = String(r.sub_ficha || '').trim();
    if (!dishName || !sfName) continue;
    if (!dishes[dishName]) dishes[dishName] = { id: slugify(dishName), name: dishName, description: '', photos: [], louca: '', equipamentos: [], sub_fichas: [], markup: 300 };
    const dish = dishes[dishName];
    let sf = dish.sub_fichas.find(s => s.name === sfName);
    if (!sf) {
      sf = { id: uid(), name: sfName, rendimento: '', rendimento_qty: null, rendimento_unit: '', ingredientes: [], modo_preparo: '' };
      dish.sub_fichas.push(sf);
    }
    const rq = parseFloat(r.rendimento_qty);
    const ru = String(r.rendimento_unit || '').trim().toLowerCase();
    if (!isNaN(rq) && ru) {
      sf.rendimento_qty = rq;
      sf.rendimento_unit = ru;
      sf.rendimento = `${String(rq).replace('.', ',')} ${ru}`;
    }
    if (r.modo_preparo && !sf.modo_preparo) sf.modo_preparo = String(r.modo_preparo).trim();
    const insumoName = String(r.insumo || '').trim();
    if (!insumoName) continue;
    const qty = parseFloat(r.qtd);
    const unit = String(r.unidade || '').trim().toLowerCase();
    const fcExtra = parseFloat(r.fc);
    const obs = String(r.observacao || '').trim();
    const variacao = String(r.variacao || '').trim();
    const subref = String(r.subref || '').trim();

    const ing = { insumo_name: insumoName, qty: isNaN(qty) ? null : qty, unit, observacao: obs };
    // Referência a outra sub-ficha?
    if (subref) {
      const targetSf = dish.sub_fichas.find(s => s.name === subref);
      if (targetSf) {
        ing.subref_id = targetSf.id;
        ing.insumo_id = '';
      }
    } else {
      const insMatch = insumosByName[insumoName.toLowerCase()];
      ing.insumo_id = insMatch ? insMatch.id : slugify(insumoName);
      if (variacao && insMatch && insMatch.variations) {
        const v = insMatch.variations.find(x => x.name.toLowerCase() === variacao.toLowerCase());
        if (v) {
          ing.variation_name = v.name;
          ing.fc = v.fc;
        }
      }
      if (!ing.fc && !isNaN(fcExtra) && fcExtra > 1) ing.fc = fcExtra;
    }
    sf.ingredientes.push(ing);
  }

  const equipamentos = rawEquipamentos
    .map(r => String(r.nome || '').trim())
    .filter(Boolean);

  return {
    insumos: Object.values(insumosByName),
    dishes: Object.values(dishes),
    equipamentos,
  };
}

// Aplica import: mode = 'append' (merge) ou 'replace' (apaga tudo e seta)
async function applyExcelImport(cid, parsed, mode) {
  let batch = writeBatch(db);
  let count = 0;
  const flush = async () => { if (count > 0) { await batch.commit(); batch = writeBatch(db); count = 0; } };
  const push = async (op, ref, data) => {
    if (op === 'set') batch.set(ref, data);
    else if (op === 'delete') batch.delete(ref);
    count++;
    if (count >= 400) await flush();
  };

  if (mode === 'replace') {
    const [dSnap, iSnap] = await Promise.all([getDocs(dishesCol(cid)), getDocs(insumosCol(cid))]);
    for (const d of dSnap.docs) await push('delete', dishDoc(cid, d.id));
    for (const d of iSnap.docs) await push('delete', insumoDoc(cid, d.id));
  }

  for (const ins of parsed.insumos) {
    const clean = { ...ins }; delete clean.id;
    await push('set', insumoDoc(cid, ins.id), clean);
  }
  for (const dish of parsed.dishes) {
    const clean = { ...dish }; delete clean.id;
    await push('set', dishDoc(cid, dish.id), clean);
  }
  // Equipamentos: sempre faz merge (não apaga os existentes em replace, a menos que a planilha tenha dados)
  if (parsed.equipamentos.length > 0) {
    if (mode === 'replace') {
      await push('set', configDoc(cid, 'equipamentos'), { list: parsed.equipamentos });
    } else {
      // Append: une com os existentes
      const existing = await getDoc(configDoc(cid, 'equipamentos'));
      const existingList = existing.exists() ? (existing.data().list || []) : [];
      const merged = [...new Set([...existingList, ...parsed.equipamentos])];
      await push('set', configDoc(cid, 'equipamentos'), { list: merged });
    }
  }
  await flush();
}

function openImportExcelModal(cid, clienteName) {
  let parsed = null;
  const modal = el('div', { class: 'modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() })
  );
  const content = el('div', { class: 'modal-content modal-wide' });
  content.appendChild(el('h2', {}, `Importar de Excel — ${clienteName || cid}`));
  content.appendChild(el('p', { class: 'modal-subtitle' },
    'Envie uma planilha .xlsx com as abas Insumos, Variacoes, Fichas e Equipamentos. ' +
    'Baixe o modelo abaixo se for sua primeira vez.'));

  content.appendChild(el('button', {
    class: 'btn btn-small',
    onclick: () => downloadExcelTemplate()
  }, '↓ Baixar modelo .xlsx'));

  const fileInput = el('input', { type: 'file', accept: '.xlsx,.xls', class: 'import-file-input' });
  const previewArea = el('div', { class: 'import-preview' });
  fileInput.addEventListener('change', async () => {
    const f = fileInput.files[0];
    if (!f) return;
    previewArea.innerHTML = '';
    previewArea.appendChild(el('p', { class: 'muted' }, 'Lendo planilha...'));
    try {
      parsed = await parseExcelFile(f);
      previewArea.innerHTML = '';
      previewArea.appendChild(el('div', { class: 'import-summary' },
        el('p', {}, el('strong', {}, `${parsed.insumos.length} insumos`), ' serão criados/atualizados'),
        el('p', {}, el('strong', {}, `${parsed.insumos.reduce((s, i) => s + ((i.variations || []).length), 0)} variações`), ' cadastradas em insumos'),
        el('p', {}, el('strong', {}, `${parsed.dishes.length} fichas`), ` (${parsed.dishes.reduce((s, d) => s + d.sub_fichas.length, 0)} sub-fichas)`),
        el('p', {}, el('strong', {}, `${parsed.equipamentos.length} equipamentos`))
      ));
      // Lista de fichas pra preview
      if (parsed.dishes.length > 0) {
        const ul = el('ul', { class: 'import-dish-list' });
        parsed.dishes.forEach(d => {
          ul.appendChild(el('li', {}, `${d.name} — ${d.sub_fichas.length} sub-fichas, ${d.sub_fichas.reduce((s, sf) => s + sf.ingredientes.length, 0)} ingredientes`));
        });
        previewArea.appendChild(el('details', {}, el('summary', {}, 'Ver fichas importadas'), ul));
      }
    } catch (err) {
      previewArea.innerHTML = '';
      previewArea.appendChild(el('p', { class: 'error' }, 'Erro ao ler planilha: ' + err.message));
    }
  });
  content.appendChild(fileInput);
  content.appendChild(previewArea);

  // Modo: append vs replace
  const modeWrap = el('div', { class: 'import-mode' },
    el('label', { class: 'field' },
      el('input', { type: 'radio', name: 'import-mode', value: 'append', checked: true }),
      el('span', {}, ' ADICIONAR — mantém os dados existentes e insere os da planilha')
    ),
    el('label', { class: 'field' },
      el('input', { type: 'radio', name: 'import-mode', value: 'replace' }),
      el('span', {}, ' SUBSTITUIR — apaga tudo e insere só o que está na planilha')
    )
  );
  content.appendChild(modeWrap);

  content.appendChild(el('div', { class: 'modal-actions' },
    el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
    el('button', { class: 'btn btn-primary', onclick: async () => {
      if (!parsed) { alert('Selecione uma planilha primeiro'); return; }
      const mode = content.querySelector('input[name="import-mode"]:checked').value;
      if (mode === 'replace' && !confirm('Tem certeza? Isso APAGA tudo que existe e insere só o que está na planilha.')) return;
      try {
        toast('Importando...');
        await applyExcelImport(cid, parsed, mode);
        toast('Importação concluída!');
        modal.remove();
        if (typeof renderClientesAdmin === 'function') renderClientesAdmin();
      } catch (err) { toast('Erro: ' + err.message); }
    } }, 'Importar')
  ));
  modal.appendChild(content);
  document.body.appendChild(modal);
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
    return;
  }
  // Verifica se há pré-cadastro pendente com este email
  try {
    const slug = emailSlug(user.email);
    const pendingRef = doc(db, 'pending_users', slug);
    const pendingSnap = await getDoc(pendingRef);
    if (pendingSnap.exists() && pendingSnap.data().email === user.email) {
      const pData = pendingSnap.data();
      const newDoc = {
        email: user.email,
        name: pData.name || user.email,
        role: pData.role || 'cliente_op',
        clienteIds: pData.clienteIds || [],
        whatsapp: pData.whatsapp || '',
        mustChangePassword: pData.mustChangePassword !== false, // default true
        createdAt: serverTimestamp(),
        claimedFromPending: true
      };
      await setDoc(ref, newDoc);
      // Apaga o pre-claim
      try { await deleteDoc(pendingRef); } catch (e) { console.warn('delete pending:', e); }
      STATE.userDoc = { uid: user.uid, ...newDoc };
      return;
    }
  } catch (err) { console.warn('check pending_users:', err); }
  STATE.userDoc = null;
}

// Modal: oferece pré-cadastro quando o email já existe no Firebase Auth mas não tem doc Firestore.
// Quando a pessoa logar com a senha que ela já tem, o ensureUserDoc cria o doc automaticamente.
async function offerPendingEnrollment({ email, name, role, clienteIds, whatsapp }) {
  return new Promise((resolve) => {
    const existing = $('#pending-enroll-modal');
    if (existing) existing.remove();

    async function doPreClaim() {
      const slug = emailSlug(email);
      try {
        await setDoc(doc(db, 'pending_users', slug), {
          email, name, role, clienteIds,
          whatsapp: whatsapp || '',
          mustChangePassword: false, // já tem senha — não força troca
          createdBy: STATE.user?.uid || 'master',
          createdAt: new Date().toISOString()
        });
        toast('Pré-cadastro salvo');
        showPreClaimSuccessModal(email, name);
        modal.remove();
        renderUsuariosAdmin();
        resolve();
      } catch (err) { alert('Erro ao pré-cadastrar: ' + (err.message || err.code)); }
    }

    const modal = el('div', { class: 'modal', id: 'pending-enroll-modal' },
      el('div', { class: 'modal-overlay', onclick: () => { modal.remove(); resolve(); } }),
      el('div', { class: 'modal-content modal-content-wide' },
        el('h2', {}, 'Email já existe no Firebase'),
        el('p', { class: 'modal-subtitle' }, `O email ${email} tem conta no Firebase Auth, mas não tem registro no sistema (foi excluído antes ou nunca foi vinculado).`),
        el('div', { style: 'background:#fef6e0;border:1px solid #e9d3a0;border-radius:8px;padding:1rem;margin:1rem 0;' },
          el('p', { style: 'margin:0 0 0.5rem;font-weight:600;color:#8a6b40;' }, 'Solução recomendada: pré-cadastro'),
          el('p', { class: 'muted', style: 'font-size:0.85rem;line-height:1.5;margin:0;' },
            'O sistema salva o cadastro (nome, papel, restaurantes, WhatsApp) numa "fila de pendentes" amarrada ao email. ',
            'Quando a pessoa entrar com a senha que ela já tem, o sistema vincula automaticamente os dados do pré-cadastro.',
            el('br', {}), el('br', {}),
            el('strong', {}, 'Se ela esqueceu a senha:'),
            ' depois do pré-cadastro, peça pra ela usar "Esqueci senha" no login pra receber um link de reset.')
        ),
        el('div', { class: 'modal-actions' },
          el('button', { class: 'btn', onclick: () => { modal.remove(); resolve(); } }, 'Cancelar'),
          el('button', { class: 'btn btn-primary', onclick: doPreClaim }, 'Pré-cadastrar este email')
        )
      )
    );
    document.body.appendChild(modal);
  });
}

function showPreClaimSuccessModal(email, name) {
  const existing = $('#pre-claim-success-modal');
  if (existing) existing.remove();
  const modal = el('div', { class: 'modal', id: 'pre-claim-success-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content' },
      el('h2', {}, '✓ Pré-cadastro salvo'),
      el('p', {}, `${name || email} ficou na fila de pendentes.`),
      el('p', { class: 'muted', style: 'font-size:0.88rem;' },
        'Avise a pessoa que ela pode entrar no sistema com o email ',
        el('strong', {}, email),
        ' e a senha que ela já usava. Se esqueceu, pode usar "Esqueci senha" no login.'
      ),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn btn-primary', onclick: () => modal.remove() }, 'Entendi')
      )
    )
  );
  document.body.appendChild(modal);
}

// Tela de troca obrigatória de senha (primeiro login após convite)
function renderForcePasswordChange() {
  const app = $('#app');
  app.innerHTML = '';
  const userDoc = STATE.userDoc || {};
  const box = el('section', { class: 'trial-block' },
    el('div', { class: 'trial-block-card' },
      el('h1', {}, 'Defina sua senha'),
      el('p', {}, 'Bem-vindo(a)' + (userDoc.name ? ', ' + userDoc.name.split(' ')[0] : '') + '!'),
      el('p', { class: 'muted' }, 'Este é seu primeiro acesso. Crie uma senha pessoal pra continuar.'),
      (() => {
        const form = el('form', { class: 'password-change-form', onsubmit: (e) => e.preventDefault() });
        const newPw = el('input', { type: 'password', placeholder: 'Nova senha (mínimo 6 caracteres)', required: '', minlength: '6' });
        const confirmPw = el('input', { type: 'password', placeholder: 'Repita a nova senha', required: '', minlength: '6' });
        const errEl = el('p', { class: 'pw-error', style: 'display:none;color:#a31e1e;font-size:0.85rem;margin-top:0.5rem;' });
        const submitBtn = el('button', { class: 'btn btn-primary', type: 'submit' }, 'Salvar nova senha');
        form.appendChild(el('label', { class: 'field', style: 'margin-top:1rem;' }, el('span', { class: 'label-text' }, 'Nova senha'), newPw));
        form.appendChild(el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Confirmar senha'), confirmPw));
        form.appendChild(errEl);
        form.appendChild(submitBtn);
        function showErr(msg) { errEl.textContent = msg; errEl.style.display = ''; }
        submitBtn.addEventListener('click', async () => {
          errEl.style.display = 'none';
          const a = newPw.value; const b = confirmPw.value;
          if (a.length < 6) { showErr('Senha precisa ter pelo menos 6 caracteres'); return; }
          if (a !== b) { showErr('As senhas não conferem'); return; }
          submitBtn.disabled = true;
          submitBtn.textContent = 'Salvando...';
          try {
            await updatePassword(auth.currentUser, a);
            await setDoc(doc(db, 'users', auth.currentUser.uid), { mustChangePassword: deleteField(), passwordChangedAt: new Date().toISOString() }, { merge: true });
            if (STATE.userDoc) { delete STATE.userDoc.mustChangePassword; }
            toast('Senha atualizada');
            route();
          } catch (err) {
            submitBtn.disabled = false;
            submitBtn.textContent = 'Salvar nova senha';
            if (err.code === 'auth/requires-recent-login') {
              showErr('Sua sessão expirou. Faça login de novo e tente novamente.');
              setTimeout(() => doLogout(), 2500);
            } else {
              showErr('Erro: ' + (err.message || err.code));
            }
          }
        });
        return form;
      })(),
      el('button', { class: 'btn', style: 'margin-top:1rem;', onclick: doLogout }, 'Sair sem trocar')
    )
  );
  app.appendChild(box);
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
    nav.classList.add('logged');
    const navClientes = $('#nav-clientes');
    const navUsuarios = $('#nav-usuarios');
    navClientes && (navClientes.style.display = (isMaster() || isStaff()) ? '' : 'none');
    navUsuarios && (navUsuarios.style.display = canManageUsers() ? '' : 'none');
  } else {
    authBtn.textContent = 'Entrar';
    authBtn.classList.remove('logged-in');
    nav.classList.remove('logged');
  }
  updateBreadcrumb();
}
function updateBreadcrumb() {
  const el = $('#brand-cliente');
  if (!el) return;
  if (STATE.currentCliente && STATE.currentClienteId) {
    el.textContent = ' · ' + STATE.currentCliente.name;
    el.style.display = '';
  } else {
    el.textContent = '';
    el.style.display = 'none';
  }
}

// ---------- Router ----------
function parseHash() {
  const h = location.hash.slice(1) || '/';
  return h.split('/').filter(Boolean);
}

async function route() {
  // Hashes que são âncoras de scroll (não rotas): sf-XYZ, top, etc.
  // Não dispara render — só scroll pro elemento.
  const rawHash = location.hash.slice(1);
  if (rawHash.startsWith('sf-')) {
    const target = document.getElementById(rawHash);
    if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    return;
  }
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

  // Usuário desativado (soft delete)
  if (STATE.userDoc.disabled === true) {
    renderUserDisabled();
    return;
  }

  // First-login: força troca de senha antes de qualquer rota
  if (STATE.userDoc.mustChangePassword) {
    renderForcePasswordChange();
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

    // Trial gate: bloqueia cliente fora do master/staff se trial expirado ou não iniciado
    if (trialGate(cid)) return;

    const rest = parts.slice(2);
    if (rest.length === 0) {
      renderClienteHome(cid); return;
    }
    if (rest[0] === 'ficha' && rest[1]) {
      if (!canViewCardapio(cid)) { renderNoAccess(); return; }
      renderFicha(cid, rest[1], rest[2] || null); return;
    }
    if (rest[0] === 'insumos') {
      if (!canViewInsumosTab(cid)) { renderNoAccess(); return; }
      renderInsumos(cid); return;
    }
    if (rest[0] === 'producao') {
      if (!canViewProducao(cid)) { renderNoAccess(); return; }
      renderProducao(cid); return;
    }
    if (rest[0] === 'estoque') {
      // Aba Estoque desativada — redireciona pro cardápio
      location.hash = `#/c/${cid}`;
      return;
    }
    if (rest[0] === 'admin') {
      // master/staff: acesso total; cliente (dono): pode criar/editar fichas próprias
      if (!canCreateDish(cid)) { renderNoAccess(); return; }
      if (rest[1] === 'new') { renderAdminEdit(cid, null); return; }
      if (rest[1] === 'edit' && rest[2]) { renderAdminEdit(cid, rest[2]); return; }
      renderAdminList(cid); return;
    }
    if (rest[0] === 'equipe') {
      // Cliente (dono) gerencia equipe do próprio restaurante
      if (!canManageClientTeam(cid)) { renderNoAccess(); return; }
      renderClientTeamAdmin(cid); return;
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
    el('h1', {}, 'AppMise'),
    el('p', { class: 'subtitle' }, 'Fichas técnicas e gestão de cardápio'),
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

function renderUserDisabled() {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(el('section', { class: 'trial-block' },
    el('div', { class: 'trial-block-card' },
      el('h1', {}, 'Acesso desativado'),
      el('p', {}, 'Seu acesso ao sistema foi desativado por um administrador.'),
      el('p', { class: 'muted' }, 'Entre em contato com a consultoria para regularizar.'),
      el('button', { class: 'btn', onclick: doLogout }, 'Sair')
    )
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
    const targetHash = `#/c/${c.id}`;
    const card = el('a', { class: 'cliente-card', href: targetHash, role: 'button' },
      el('h3', {}, c.name || c.id),
      el('p', { class: 'muted' }, c.slug || c.id),
      el('span', { class: 'cliente-card-arrow', 'aria-hidden': 'true' }, '→')
    );
    // Fallback de navegação caso o tap nativo no <a> falhe (Safari mobile com :hover sticky)
    card.addEventListener('click', (e) => {
      e.preventDefault();
      if (location.hash !== targetHash) location.hash = targetHash;
      else route();
    });
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
    const tStatus = getTrialStatus(c);
    let badge = null;
    if (tStatus.paid) badge = el('span', { class: 'cliente-trial-badge paid' }, '● Pago');
    else if (!tStatus.started) badge = el('span', { class: 'cliente-trial-badge not-started' }, '○ Não iniciado');
    else if (tStatus.isBlocked) badge = el('span', { class: 'cliente-trial-badge expired' }, '✕ Expirado');
    else badge = el('span', { class: 'cliente-trial-badge active' }, `● ${tStatus.daysLeft}d`);
    const card = el('article', { class: 'cliente-admin-card' },
      el('div', { class: 'cc-head' },
        el('h4', { class: 'cc-name' }, c.name, ' ', badge),
        el('span', { class: 'cc-id' }, c.id)
      ),
      el('div', { class: 'cc-actions' },
        el('a', { class: 'btn btn-primary btn-block', href: `#/c/${c.id}` }, 'Abrir restaurante →'),
        el('div', { class: 'cc-menu' },
          el('button', { class: 'btn btn-small', onclick: () => openEditClienteModal(c), title: 'Editar nome, consultor e configurações' }, '✎ Editar'),
          el('button', { class: 'btn btn-small', onclick: () => openImportExcelModal(c.id, c.name), title: 'Importar/adicionar fichas e insumos via planilha Excel' }, '↑ Importar Excel'),
          el('button', { class: 'btn btn-small btn-accent', onclick: () => openUpdateFromFileModal(c), title: 'Sincronizar do arquivo data.json' }, '↻ Atualizar do arquivo'),
          el('button', { class: 'btn btn-small', onclick: async () => {
            // Backup do cliente (precisa garantir que estamos subscritos a ele)
            try {
              const [dSnap, iSnap, cfgSnap] = await Promise.all([
                getDocs(dishesCol(c.id)),
                getDocs(insumosCol(c.id)),
                getDoc(configDoc(c.id, 'equipamentos'))
              ]);
              const payload = {
                cliente: c,
                dishes: dSnap.docs.map(d => ({ id: d.id, ...d.data() })),
                insumos: iSnap.docs.map(d => ({ id: d.id, ...d.data() })),
                equipamentos_disponiveis: cfgSnap.exists() ? (cfgSnap.data().list || []) : []
              };
              const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
              const url = URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url; a.download = `backup-${c.id}-${new Date().toISOString().slice(0,10)}.json`;
              a.click(); URL.revokeObjectURL(url);
              toast('Backup baixado');
            } catch (err) { toast('Erro: ' + err.message); }
          }, title: 'Baixar JSON com tudo deste cliente' }, '↓ Backup'),
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
  const [usersSnap, pendingSnap] = await Promise.all([
    getDocs(collection(db, 'users')),
    getDocs(collection(db, 'pending_users')).catch(() => ({ docs: [] }))
  ]);
  await loadClientesList();
  const users = usersSnap.docs.map(d => ({ uid: d.id, ...d.data() }));
  const pendings = pendingSnap.docs.map(d => ({ slug: d.id, ...d.data() }));
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
    el('p', { class: 'muted' }, 'Ao salvar, o sistema gera uma senha temporária. Você copia ou envia direto pelo WhatsApp. No primeiro login, a pessoa será obrigada a trocar a senha.')
  );

  const nameInput = el('input', { type: 'text', placeholder: 'Nome completo', required: true });
  const emailInput = el('input', { type: 'email', placeholder: 'email@exemplo.com', required: true });
  const whatsInput = el('input', { type: 'tel', placeholder: '(11) 91234-5678' });
  const roleSelect = el('select', {},
    el('optgroup', { label: 'Minha equipe (consultoria)' },
      el('option', { value: 'master' }, 'Master — acesso total ao sistema'),
      el('option', { value: 'staff' }, 'Equipe — cria/edita fichas e preços')
    ),
    el('optgroup', { label: 'Lado do cliente (restaurante)' },
      el('option', { value: 'cliente' }, 'Cliente (dono) — edita preços e cria fichas próprias'),
      el('option', { value: 'cliente_admin' }, 'Equipe Admin do Cliente — vê preços, planeja produção'),
      el('option', { value: 'cliente_op' }, 'Equipe Operacional — só fichas e produção (sem preços)')
    )
  );
  roleSelect.value = 'cliente_op';
  const clienteChecks = el('div', { class: 'cliente-chip-picker' });
  STATE.clientes.forEach(c => {
    const input = el('input', { type: 'checkbox', value: c.id, id: 'inv-c-' + c.id });
    clienteChecks.appendChild(el('label', { class: 'cliente-chip-option', for: 'inv-c-' + c.id }, input, document.createTextNode(c.name)));
  });
  invitePanel.appendChild(el('div', { class: 'form-grid' },
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Email'), emailInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'WhatsApp'), whatsInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Papel'), roleSelect)
  ));
  invitePanel.appendChild(el('label', { class: 'field' },
    el('span', { class: 'label-text' }, 'Restaurantes autorizados (obrigatório para Equipe e Cliente)'),
    clienteChecks
  ));
  invitePanel.appendChild(el('div', { class: 'panel-actions' },
    el('button', { class: 'btn btn-primary', onclick: async () => {
      const email = emailInput.value.trim();
      const name = nameInput.value.trim() || email.split('@')[0];
      const whatsapp = whatsInput.value.trim();
      const role = roleSelect.value;
      const selected = $$('input[type=checkbox]:checked', clienteChecks).map(i => i.value);
      if (!name) { alert('Informe o nome'); return; }
      if (!email) { alert('Informe o email'); return; }
      if (role !== 'master' && selected.length === 0) { alert('Selecione ao menos um restaurante'); return; }
      try {
        await inviteUser(email, name, role, selected, whatsapp);
      } catch (err) { alert('Erro: ' + err.message); }
    } }, 'Criar usuário')
  ));
  app.appendChild(invitePanel);

  // Pré-cadastros pendentes (esperando primeiro login)
  if (pendings.length > 0) {
    app.appendChild(el('h3', { class: 'section-title' }, `Pré-cadastros pendentes (${pendings.length})`));
    app.appendChild(el('p', { class: 'muted', style: 'margin-bottom:1rem;font-size:0.88rem;' },
      'Aguardando primeiro login da pessoa. Assim que ela entrar com o email + senha que já tem no Firebase, vira usuário ativo automaticamente.'
    ));
    const pendingGrid = el('div', { class: 'user-admin-list' });
    pendings.forEach(p => {
      const card = el('article', { class: 'user-card' },
        el('div', { class: 'user-card-head' },
          el('div', { class: 'user-id' },
            el('h4', { class: 'user-name' }, p.name || '—',
              el('span', { class: 'pending-badge' }, '⏳ aguardando login')),
            el('span', { class: 'user-email' }, p.email),
            p.whatsapp ? el('span', { class: 'user-whatsapp' }, '📱 ' + p.whatsapp) : null
          ),
          el('span', { class: 'role-badge role-' + (p.role || 'none') }, roleLabel(p.role))
        ),
        el('div', { class: 'user-actions' },
          el('button', { class: 'btn btn-small', onclick: async () => {
            if (!confirm(`Enviar reset de senha para ${p.email}? A pessoa recebe um email pra criar nova senha.`)) return;
            try {
              await sendPasswordResetEmail(auth, p.email);
              toast('Email de reset enviado');
            } catch (err) { alert('Erro: ' + (err.message || err.code)); }
          } }, '✉ Reset de senha'),
          el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
            if (!confirm(`Cancelar o pré-cadastro de ${p.email}?\n\nIsso só apaga a "reserva" do nosso lado — não mexe na conta dela no Firebase.`)) return;
            try {
              await deleteDoc(doc(db, 'pending_users', p.slug));
              toast('Pré-cadastro cancelado');
              renderUsuariosAdmin();
            } catch (err) { alert('Erro: ' + (err.message || err.code)); }
          } }, '✕ Cancelar')
        )
      );
      pendingGrid.appendChild(card);
    });
    app.appendChild(pendingGrid);
  }

  // Existing users — agrupado por papel
  app.appendChild(el('h3', { class: 'section-title' }, `Usuários cadastrados (${users.length})`));
  if (users.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' },
      el('p', { class: 'muted' }, 'Nenhum usuário ainda.')
    ));
    return;
  }

  // Resolve role efetivo (cliente_admin/op vs equipe legado)
  function effectiveRole(u) {
    if (u.role !== 'equipe') return u.role;
    return u.permissions?.can_view_insumos ? 'cliente_admin' : 'cliente_op';
  }
  const roleOrder = ['master', 'staff', 'cliente', 'cliente_admin', 'cliente_op'];
  const groups = {};
  users.forEach(u => {
    const r = effectiveRole(u) || 'outros';
    if (!groups[r]) groups[r] = [];
    groups[r].push(u);
  });
  const groupedIds = roleOrder.filter(r => groups[r]?.length).concat(
    Object.keys(groups).filter(r => !roleOrder.includes(r))
  );

  groupedIds.forEach(roleId => {
    const list = groups[roleId];
    // ordena: ativos primeiro, depois desativados, ambos por nome
    list.sort((a, b) => {
      if (!!a.disabled !== !!b.disabled) return a.disabled ? 1 : -1;
      return (a.name || a.email || '').localeCompare(b.name || b.email || '', 'pt-BR');
    });
    const section = el('section', { class: 'user-role-section' });
    section.appendChild(el('div', { class: 'user-role-header' },
      el('span', { class: 'role-section-dot role-' + roleId }),
      el('h4', { class: 'user-role-title' }, roleLabel(roleId)),
      el('span', { class: 'user-role-count' }, `${list.length}`)
    ));
    const grid = el('div', { class: 'user-admin-list' });
    list.forEach(u => grid.appendChild(renderUserCard(u)));
    section.appendChild(grid);
    app.appendChild(section);
  });
}

function roleLabel(role) {
  return ROLE_LABELS[role] || role || '—';
}

function renderUserCard(u) {
  const isMe = u.uid === STATE.user?.uid;
  const card = el('article', { class: 'user-card' + (u.disabled ? ' user-card-disabled' : '') });
  const head = el('div', { class: 'user-card-head' },
    el('div', { class: 'user-id' },
      el('h4', { class: 'user-name' }, u.name || '—',
        u.disabled ? el('span', { class: 'disabled-badge', title: 'Usuário desativado — não consegue acessar o sistema' }, '⏸ inativo') : null,
        !u.disabled && u.mustChangePassword ? el('span', { class: 'pending-badge', title: 'Usuário ainda não fez o primeiro login (não trocou a senha temporária)' }, '⏳ aguarda 1º login') : null
      ),
      el('span', { class: 'user-email' }, u.email),
      u.whatsapp ? el('span', { class: 'user-whatsapp' }, '📱 ' + u.whatsapp) : null
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
  } else if (u.disabled) {
    // Usuário desativado: botão de reativar + remoção definitiva
    actions.appendChild(el('button', { class: 'btn btn-small btn-primary', onclick: async () => {
      try {
        await setDoc(doc(db, 'users', u.uid), { disabled: deleteField(), disabledAt: deleteField(), reactivatedAt: new Date().toISOString() }, { merge: true });
        toast('Usuário reativado');
        renderUsuariosAdmin();
      } catch (err) { alert('Erro ao reativar: ' + (err.message || err.code)); }
    } }, '↻ Reativar'));
    actions.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
      if (!confirm(`Excluir DEFINITIVAMENTE o registro de ${u.email}?\n\nEssa ação apaga o doc no Firestore. A conta de email no Firebase Auth continua existindo (limitação técnica — só pode ser apagada no console do Firebase).\n\nSe a pessoa tentar criar conta nova com este email vai dar conflito. Recomendado: usar Reativar em vez de excluir.\n\nContinuar mesmo assim?`)) return;
      try { await deleteDoc(doc(db, 'users', u.uid)); toast('Excluído'); renderUsuariosAdmin(); }
      catch (err) { alert('Erro: ' + (err.message || err.code)); }
    } }, '✕ Excluir definitivo'));
  } else {
    actions.appendChild(el('button', { class: 'btn btn-small btn-primary', onclick: () => openEditUserModal(u) }, 'Editar'));
    actions.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
      if (!confirm(`Desativar acesso de ${u.email}?\n\nA pessoa perde o acesso ao sistema imediatamente, mas o registro fica salvo (pode ser reativado a qualquer momento sem precisar recriar).`)) return;
      try {
        await setDoc(doc(db, 'users', u.uid), { disabled: true, disabledAt: new Date().toISOString() }, { merge: true });
        toast('Usuário desativado');
        renderUsuariosAdmin();
      } catch (err) { alert('Erro: ' + (err.message || err.code)); }
    } }, 'Desativar'));
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
  const whatsInput = el('input', { type: 'tel', value: u.whatsapp || '', placeholder: '(11) 91234-5678' });
  const roleSelect = el('select', {},
    el('optgroup', { label: 'Minha equipe (consultoria)' },
      el('option', { value: 'master' }, 'Master'),
      el('option', { value: 'staff' }, 'Equipe (consultoria)')
    ),
    el('optgroup', { label: 'Lado do cliente (restaurante)' },
      el('option', { value: 'cliente' }, 'Cliente (dono)'),
      el('option', { value: 'cliente_admin' }, 'Equipe Admin do Cliente'),
      el('option', { value: 'cliente_op' }, 'Equipe Operacional do Cliente')
    ),
    el('optgroup', { label: 'Legado' },
      el('option', { value: 'equipe' }, 'Equipe (perfil legado)')
    )
  );
  roleSelect.value = u.role || 'cliente_op';

  const clienteChecks = el('div', { class: 'cliente-chip-picker' });
  const currentIds = new Set(u.clienteIds || []);
  STATE.clientes.forEach(c => {
    const input = el('input', { type: 'checkbox', value: c.id, id: 'edit-c-' + c.id });
    if (currentIds.has(c.id)) input.setAttribute('checked', '');
    clienteChecks.appendChild(el('label', { class: 'cliente-chip-option', for: 'edit-c-' + c.id }, input, document.createTextNode(c.name)));
  });

  // Permissões granulares (só aparece pra role equipe)
  const permDefs = [
    { key: 'can_view_cardapio', label: 'Ver cardápio/fichas', desc: 'Acessa a aba Cardápio e visualiza receitas' },
    { key: 'can_view_insumos', label: 'Ver insumos com preços', desc: 'Acessa a aba Insumos e vê valores' },
    { key: 'can_view_producao', label: 'Ver aba Produção', desc: 'Acessa o planejamento de produção' },
    { key: 'can_manage_contagem', label: 'Receber/gerir contagens', desc: 'Vê todas as contagens, edita, exporta e gera pedidos de compra' },
    { key: 'can_edit_contagem', label: 'Editar contagem enviada', desc: 'Pode alterar contagens já enviadas (toda mudança gera log)' },
    { key: 'can_manage_templates', label: 'Criar/editar templates', desc: 'Cria e edita templates de contagem (e checklists futuros)' },
  ];
  const currentPerms = u.permissions || {};
  const permChecks = {};
  const permBox = el('div', { class: 'permissions-grid' });
  permDefs.forEach(p => {
    const chk = el('input', { type: 'checkbox', id: 'perm-' + p.key });
    if (currentPerms[p.key]) chk.setAttribute('checked', '');
    permChecks[p.key] = chk;
    permBox.appendChild(el('label', { class: 'perm-item', for: 'perm-' + p.key }, chk,
      el('div', { class: 'perm-text' },
        el('strong', {}, p.label),
        el('span', { class: 'perm-desc' }, p.desc))));
  });
  const permSection = el('div', { class: 'perm-section' },
    el('h3', {}, 'Permissões extras (perfil legado)'),
    el('p', { class: 'muted' }, 'Estas permissões eram do antigo perfil "Equipe" com checkboxes. O sistema agora detecta automaticamente: se "Ver insumos com preços" está marcado, o usuário se comporta como Equipe Admin do Cliente; senão, como Equipe Operacional. Migre o papel pra um dos novos pra simplificar.'),
    permBox
  );
  permSection.style.display = roleSelect.value === 'equipe' ? '' : 'none';
  roleSelect.addEventListener('change', () => {
    permSection.style.display = roleSelect.value === 'equipe' ? '' : 'none';
  });

  const modal = el('div', { class: 'modal', id: 'edit-user-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content modal-content-wide' },
      el('h2', {}, 'Editar usuário'),
      el('p', { class: 'modal-subtitle' }, u.email),
      el('div', { class: 'form-grid' },
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'WhatsApp'), whatsInput),
        el('label', { class: 'field field-wide' }, el('span', { class: 'label-text' }, 'Papel'), roleSelect)
      ),
      el('label', { class: 'field' },
        el('span', { class: 'label-text' }, 'Restaurantes autorizados'),
        clienteChecks
      ),
      permSection,
      el('div', { style: 'margin-top:1rem;padding-top:1rem;border-top:1px solid #eee;' },
        el('h4', { style: 'font-size:0.78rem;text-transform:uppercase;letter-spacing:0.14em;color:#888;font-weight:500;margin-bottom:0.5rem;' }, 'Senha'),
        el('p', { class: 'muted', style: 'font-size:0.85rem;margin-bottom:0.6rem;' },
          u.mustChangePassword
            ? '⚠ Usuário ainda não trocou a senha temporária inicial.'
            : 'Reset envia um link no email do usuário pra ele definir uma nova senha.'
        ),
        el('button', { class: 'btn btn-small', onclick: async (e) => {
          e.preventDefault();
          if (!confirm(`Enviar email de reset de senha para ${u.email}?`)) return;
          try {
            await sendPasswordResetEmail(auth, u.email);
            toast('Email de reset enviado');
          } catch (err) { alert('Erro: ' + (err.message || err.code)); }
        } }, '✉ Enviar reset de senha')
      ),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: async () => {
          const role = roleSelect.value;
          const ids = $$('input[type=checkbox]:checked', clienteChecks).map(i => i.value);
          if (role !== 'master' && ids.length === 0) { alert('Selecione ao menos um restaurante'); return; }
          const permissions = {};
          if (role === 'equipe') {
            permDefs.forEach(p => {
              if (permChecks[p.key].checked) permissions[p.key] = true;
            });
          }
          try {
            await setDoc(doc(db, 'users', u.uid), {
              ...u, name: nameInput.value.trim() || u.email,
              whatsapp: whatsInput.value.trim(),
              role, clienteIds: ids,
              permissions: role === 'equipe' ? permissions : {}
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

// Gera senha temporária legível: 8 chars alfanuméricos sem caracteres confusos (0/O/1/l/I)
function generateTempPassword() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz23456789';
  let p = '';
  for (let i = 0; i < 8; i++) p += chars.charAt(Math.floor(Math.random() * chars.length));
  return p;
}

function emailSlug(email) {
  return (email || '').toLowerCase().trim().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

async function inviteUser(email, name, role, clienteIds, whatsapp) {
  // 1) Antes de criar no Auth, verifica se já existe doc Firestore com esse email (caso de soft-delete anterior)
  try {
    const existingSnap = await getDocs(query(collection(db, 'users'), where('email', '==', email)));
    if (!existingSnap.empty) {
      const existingDoc = { uid: existingSnap.docs[0].id, ...existingSnap.docs[0].data() };
      if (existingDoc.disabled) {
        if (!confirm(`O email ${email} já tem conta no sistema (foi desativada antes).\n\nDeseja REATIVAR essa conta com os novos dados (nome, papel, restaurantes, WhatsApp)?\n\nA senha continuará a mesma que a pessoa já usava. Se ela esqueceu, use "Enviar reset de senha" depois.`)) return;
        await setDoc(doc(db, 'users', existingDoc.uid), {
          ...existingDoc,
          name, role, clienteIds,
          whatsapp: whatsapp || '',
          disabled: deleteField(),
          disabledAt: deleteField(),
          reactivatedAt: new Date().toISOString()
        }, { merge: true });
        toast('Usuário reativado com novos dados');
        renderUsuariosAdmin();
        return;
      } else {
        alert(`Este email já tem conta ATIVA no sistema. Use o botão "Editar" no card do usuário para alterar dados, ou "Enviar reset de senha".`);
        return;
      }
    }
  } catch (err) { console.warn('check existing user:', err); }

  const tempPw = generateTempPassword();
  // Usa app secundário pra não afetar a sessão do master
  const secondaryApp = initializeApp(firebaseConfig, 'secondary-' + uid());
  const secondaryAuth = getAuth(secondaryApp);
  let newUid = null;
  try {
    const cred = await createUserWithEmailAndPassword(secondaryAuth, email, tempPw);
    newUid = cred.user.uid;
    await signOut(secondaryAuth);
  } catch (err) {
    if (err.code === 'auth/email-already-in-use') {
      // Email já existe no Auth (foi excluído definitivamente antes ou nunca teve doc).
      // Em vez de bloquear, oferece pré-cadastrar via pending_users — quando a pessoa logar
      // com a senha que já tem no Firebase, o sistema cria o doc users/{uid} automaticamente.
      await offerPendingEnrollment({ email, name, role, clienteIds, whatsapp });
      return;
    }
    if (err.code === 'auth/weak-password') {
      alert('Senha muito fraca — tente de novo.');
      return;
    }
    throw err;
  }
  await setDoc(doc(db, 'users', newUid), {
    email, name, role, clienteIds,
    whatsapp: whatsapp || '',
    mustChangePassword: true,
    createdAt: serverTimestamp()
  });
  toast('Usuário criado');
  showInviteCredentialsModal({ name, email, password: tempPw, whatsapp });
  renderUsuariosAdmin();
}

// Modal com credenciais geradas — admin copia ou envia pelo WhatsApp
function showInviteCredentialsModal({ name, email, password, whatsapp }) {
  const cliente = STATE.currentCliente;
  const consultorNome = cliente?.consultor_name || 'Gustavo Rodrigues';
  const appUrl = location.origin + location.pathname.replace(/\/[^/]*$/, '/');
  const message =
    `Olá, ${name}!\n\n` +
    `Seu acesso ao AppMise foi liberado.\n\n` +
    `Link: ${appUrl}\n` +
    `Email: ${email}\n` +
    `Senha temporária: ${password}\n\n` +
    `No primeiro acesso o sistema vai pedir pra você trocar a senha por uma de sua escolha.\n\n` +
    `Qualquer dúvida, é só falar.\n` +
    `— ${consultorNome}`;

  const existing = $('#invite-cred-modal');
  if (existing) existing.remove();

  const credBox = el('div', { class: 'invite-cred-box' });
  credBox.appendChild(el('div', { class: 'invite-cred-row' },
    el('span', { class: 'invite-cred-label' }, 'Email'),
    el('code', { class: 'invite-cred-value' }, email)
  ));
  credBox.appendChild(el('div', { class: 'invite-cred-row' },
    el('span', { class: 'invite-cred-label' }, 'Senha temporária'),
    el('code', { class: 'invite-cred-value invite-cred-password' }, password)
  ));

  const msgTextarea = el('textarea', { class: 'invite-msg', readonly: '', rows: '10' });
  msgTextarea.value = message;

  function copyText(text, label) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(() => toast(label + ' copiado'), () => fallback());
    } else { fallback(); }
    function fallback() {
      msgTextarea.select();
      try { document.execCommand('copy'); toast(label + ' copiado'); }
      catch { toast('Use Cmd/Ctrl+C pra copiar'); }
    }
  }

  function onlyDigits(s) { return (s || '').replace(/\D/g, ''); }
  const waDigits = onlyDigits(whatsapp);
  const waPhone = waDigits.startsWith('55') || waDigits.length < 11 ? waDigits : ('55' + waDigits);
  const waLink = waPhone ? `https://wa.me/${waPhone}?text=${encodeURIComponent(message)}` : null;

  const actions = el('div', { class: 'invite-cred-actions' },
    el('button', { class: 'btn btn-primary', onclick: () => copyText(message, 'Mensagem') }, '⧉ Copiar mensagem'),
    el('button', { class: 'btn', onclick: () => copyText(password, 'Senha') }, '⧉ Só a senha'),
    waLink
      ? el('a', { class: 'btn btn-accent', href: waLink, target: '_blank', rel: 'noopener' }, '✉ Enviar pelo WhatsApp')
      : el('button', { class: 'btn btn-accent', disabled: '', title: 'Cadastre o WhatsApp do usuário pra usar este botão' }, '✉ WhatsApp (sem número)')
  );

  const modal = el('div', { class: 'modal', id: 'invite-cred-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content modal-content-wide' },
      el('h2', {}, '✓ Usuário criado: ' + name),
      el('p', { class: 'modal-subtitle' }, 'Envie estas credenciais pelo canal que preferir. A pessoa será obrigada a trocar a senha no primeiro login.'),
      credBox,
      el('p', { class: 'muted', style: 'margin:0.8rem 0 0.3rem;font-size:0.85rem;' }, 'Mensagem pronta:'),
      msgTextarea,
      actions,
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Fechar')
      )
    )
  );
  document.body.appendChild(modal);
}

// ---------- Unidade canônica: helpers ----------
const CANONICAL_UNITS = ['kg', 'l', 'und', 'folhas', 'fatias', 'colher de sopa'];
function unitCategory(u) {
  const s = (u || '').toLowerCase().trim();
  if (['kg','g','mg','gr'].includes(s)) return 'weight';
  if (['l','ml','lt'].includes(s)) return 'volume';
  if (['und','unidade','unidades'].includes(s)) return 'unit';
  if (['folha','folhas'].includes(s)) return 'folhas';
  if (['fatia','fatias'].includes(s)) return 'fatias';
  if (s === 'colher de sopa') return 'colher de sopa';
  return s;
}
function buildUnitSelect(currentUnit) {
  const sel = el('select', { class: 'unit-select' });
  const cur = (currentUnit || 'kg').toLowerCase().trim();
  const opts = [...CANONICAL_UNITS];
  if (cur && !opts.includes(cur)) opts.push(cur);
  opts.forEach(u => {
    const o = el('option', { value: u }, u);
    if (u === cur) o.setAttribute('selected', '');
    sel.appendChild(o);
  });
  return sel;
}

// ---------- Cost calculations (same core as before) ----------
function findInsumo(id) { return STATE.insumos.find(i => i.id === id); }
function nrm(s) {
  if (!s) return '';
  return String(s).trim().toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/\(.*?\)/g, '').replace(/\s+/g, ' ').trim().replace(/[,.;:\s]+$/, '');
}
// Busca um subproduto por nome em TODOS os pratos do cliente.
// Retorna { dish, sf, subproduto } ou null.
function findSubproduto(name) {
  if (!name) return null;
  const target = nrm(name);
  if (target.length < 3) return null;
  for (const dish of STATE.dishes || []) {
    for (const sf of dish.sub_fichas || []) {
      if (!sf.subproduto || !sf.subproduto.name) continue;
      if (nrm(sf.subproduto.name) === target) {
        return { dish, sf, subproduto: sf.subproduto };
      }
    }
  }
  return null;
}
// Soma consumo de um subproduto (por nome) dentro do mesmo prato — converte unidade quando possível
function computeSubprodConsumption(dish, subprodName, subprodUnit) {
  if (!subprodName) return 0;
  const target = nrm(subprodName);
  let total = 0;
  for (const sf of dish.sub_fichas || []) {
    for (const ing of sf.ingredientes || []) {
      if (ing.subref_id) continue;
      if (nrm(ing.insumo_name) !== target) continue;
      if (ing.qty == null) continue;
      const [q] = normalizeSubrefQty(ing.qty, ing.unit, subprodUnit);
      total += q;
    }
  }
  return total;
}
// Lista de todos subprodutos (global no cliente)
function listAllSubprodutos() {
  const list = [];
  for (const dish of STATE.dishes || []) {
    for (const sf of dish.sub_fichas || []) {
      if (sf.subproduto && sf.subproduto.name) {
        list.push({ dish, sf, subproduto: sf.subproduto });
      }
    }
  }
  return list;
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
// Retorna rendimento estruturado preferindo os campos rendimento_qty/unit, senão parse da string
function getSfRendimento(sf) {
  if (!sf) return { qty: 1, unit: '' };
  if (typeof sf.rendimento_qty === 'number' && sf.rendimento_qty > 0) {
    return { qty: sf.rendimento_qty, unit: (sf.rendimento_unit || '').toLowerCase().trim() };
  }
  return parseRendimentoQty(sf.rendimento);
}
function sfRendimentoText(sf, scale = 1) {
  const r = getSfRendimento(sf);
  const q = r.qty * scale;
  const n = normUnitForDisplay(q, r.unit);
  return `${n.text} ${n.unit}`.trim();
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
      const subRend = getSfRendimento(subSf);
      const ingQty = ing.qty;
      let cost = 0;
      if (ingQty != null && subRend.qty > 0) {
        const [qConverted] = normalizeSubrefQty(ingQty, ing.unit, subRend.unit);
        cost = (qConverted / subRend.qty) * subResult.total;
      }
      total += cost;
      return { ing, insumo: null, cost, subSf, isSubref: true };
    }
    // Detecta subproduto (do mesmo prato ou outro prato) — sempre custo zero
    const subprodHit = findSubproduto(ing.insumo_name);
    if (subprodHit && !(subprodHit.dish.id === dish.id && subprodHit.sf.id === sf.id)) {
      // Evita self-reference: não trata como subproduto se for a própria sub-ficha
      return { ing, insumo: null, cost: 0, isSubref: false, isSubproduto: true, subprodHit };
    }
    const insumo = findInsumo(ing.insumo_id);
    const isReutilizavel = !!(insumo && insumo.reutilizavel);
    const [qNorm, priceNorm] = normalizeQtyPrice(ing, insumo);
    const fc = (typeof ing.fc === 'number' && ing.fc > 0) ? ing.fc : 1;
    // Insumos reutilizáveis (óleo de fritura, etc) não entram no custo da ficha — vão em despesas operacionais
    const cost = isReutilizavel ? 0 : ((qNorm != null && priceNorm != null) ? qNorm * priceNorm * fc : 0);
    total += cost;
    return { ing, insumo, cost, isSubref: false, fc: isReutilizavel ? 1 : fc, isReutilizavel };
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
  const finalRend = getSfRendimento(finalSf);
  const finalScale = finalRend.qty > 0 ? finalTargetQty / finalRend.qty : 1;
  scales[finalSf.id] = finalScale;

  function propagate(sfId, scale, visited = new Set()) {
    if (visited.has(sfId)) return;
    visited.add(sfId);
    const sf = dish.sub_fichas.find(s => s.id === sfId);
    if (!sf) return;
    const sfIdx = dish.sub_fichas.findIndex(s => s.id === sfId);
    for (const ing of sf.ingredientes || []) {
      let ref = null;
      if (ing.subref_id) ref = dish.sub_fichas.find(s => s.id === ing.subref_id);
      if (!ref && sfIdx > 0) ref = detectSubref(dish, sfIdx, ing.insumo_name);
      if (!ref) continue;
      const refRend = getSfRendimento(ref);
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
  propagate(finalSf.id, finalScale);
  return scales;
}
// ---------- Views: Cliente home (cardápio) — list format ----------
function renderClienteHome(cid) {
  const app = $('#app');
  app.innerHTML = '';
  const totalSubfichas = STATE.dishes.reduce((s, d) => s + (d.sub_fichas || []).length, 0);

  app.appendChild(renderClienteContext(cid));

  const cliente = STATE.currentCliente;
  const hero = el('section', { class: 'home-hero' },
    el('h1', {}, 'Cardápio'),
    el('p', { class: 'subtitle' }, cliente?.name || ''),
    el('p', { class: 'home-stats-mini' },
      `${STATE.dishes.length} pratos · ${totalSubfichas} sub-fichas · ${STATE.insumos.length} insumos`)
  );
  app.appendChild(hero);

  // Consultoria (se ativa)
  if (cliente?.show_consultor !== false && cliente?.consultor_name) {
    app.appendChild(el('div', { class: 'consultor-credit' },
      el('span', { class: 'consultor-by' }, 'Consultoria por'),
      el('span', { class: 'consultor-name' }, cliente.consultor_name),
      cliente.consultor_info ? el('span', { class: 'consultor-info' }, cliente.consultor_info) : null
    ));
  }

  if (STATE.dishes.length === 0) {
    const empty = el('div', { class: 'empty-state' },
      el('p', {}, 'Esse restaurante ainda não tem fichas cadastradas.'),
      canEditCliente(cid) ? el('a', { class: 'btn btn-primary', href: `#/c/${cid}/admin/new` }, '+ Criar primeira ficha') : null,
      canEditCliente(cid) ? el('p', { class: 'muted', style: 'margin-top:1rem;font-size:0.85rem;' }, 'Você também pode importar de uma planilha Excel pela tela "Restaurantes".') : null,
    );
    app.appendChild(empty);
    return;
  }

  const listWrap = el('div', { class: 'cardapio-list' });
  STATE.dishes.forEach((dish, idx) => {
    const costInfo = dishCost(dish);
    const { costPerPortion, suggestedPrice, cmv, markup } = costInfo;
    const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
    const rendDisplay = lastSf?.rendimento ? formatRendimento(lastSf.rendimento) : '—';
    const showCost = canEditInsumoPrice(cid);
    const dishGroup = el('details', { class: 'dish-group' });
    const summary = el('summary', { class: 'dish-head dish-summary' },
      el('span', { class: 'dish-chev' }, '▸'),
      el('span', { class: 'dish-title-compact' },
        el('span', { class: 'dish-number' }, String(idx + 1).padStart(2, '0')),
        el('span', { class: 'dish-name' }, dish.name)
      ),
      el('div', { class: 'dish-meta' },
        el('span', { class: 'meta-item' }, el('em', {}, 'rendimento '), rendDisplay),
        showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'custo/porção '), fmtBRL(costPerPortion)) : null,
        showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'preço '), fmtBRL(suggestedPrice)) : null,
        showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'CMV '), fmtNum(cmv, 1) + '%') : null
      )
    );
    dishGroup.appendChild(summary);
    const body = el('div', { class: 'dish-body' });
    body.appendChild(el('div', { class: 'dish-body-actions' },
      el('a', { class: 'btn btn-small btn-primary', href: `#/c/${cid}/ficha/${dish.id}` }, 'Ver ficha completa →')
    ));
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
    body.appendChild(ul);
    dishGroup.appendChild(body);
    listWrap.appendChild(dishGroup);
  });
  app.appendChild(listWrap);
}

function renderClienteContext(cid) {
  const wrap = el('div', { class: 'cliente-nav' });
  // Back link (só para master/staff) em linha superior separada
  if (isMaster() || isStaff()) {
    wrap.appendChild(el('a', { href: '#/clientes', class: 'cliente-back' }, '← Todos os restaurantes'));
  }
  // Banner de trial (se aplicável)
  const trialBanner = renderTrialBanner(cid);
  if (trialBanner) wrap.appendChild(trialBanner);
  // Tabs com active state baseado na rota atual
  const hash = location.hash.slice(1) || '/';
  const isAdmin = hash.includes(`/c/${cid}/admin`);
  const isInsumos = hash.includes(`/c/${cid}/insumos`);
  const isProducao = hash.includes(`/c/${cid}/producao`);
  const isCardapio = !isAdmin && !isInsumos && !isProducao;
  const tabs = el('nav', { class: 'cliente-tabs' },
    canViewCardapio(cid) ? el('a', { class: 'cliente-tab' + (isCardapio ? ' active' : ''), href: `#/c/${cid}` },
      el('span', { class: 'tab-icon' }, '◉'), 'Cardápio') : null,
    canViewProducao(cid) ? el('a', { class: 'cliente-tab' + (isProducao ? ' active' : ''), href: `#/c/${cid}/producao` },
      el('span', { class: 'tab-icon' }, '▲'), 'Produção') : null,
    canViewInsumosTab(cid) ? el('a', { class: 'cliente-tab' + (isInsumos ? ' active' : ''), href: `#/c/${cid}/insumos` },
      el('span', { class: 'tab-icon' }, '◎'), 'Insumos') : null,
    canManageClientTeam(cid) ? el('a', { class: 'cliente-tab' + (hash.includes(`/c/${cid}/equipe`) ? ' active' : ''), href: `#/c/${cid}/equipe` },
      el('span', { class: 'tab-icon' }, '☱'), 'Equipe') : null,
    canCreateDish(cid) ? el('a', { class: 'cliente-tab' + (isAdmin ? ' active' : ''), href: `#/c/${cid}/admin` },
      el('span', { class: 'tab-icon' }, '⚙'), 'Gerenciar') : null,
  );
  wrap.appendChild(tabs);
  return wrap;
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

  const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
  const originalFinal = getSfRendimento(finalSf);
  const state = {
    view: 'trabalho',
    finalTargetQty: originalFinal.qty,
    finalUnit: originalFinal.unit,
  };
  state.scales = () => computeCascadeScales(dish, state.finalTargetQty);

  // Header with back link, title, and inline action bar
  const header = el('div', { class: 'ficha-header-v2' },
    el('a', { class: 'back-link', href: `#/c/${cid}` }, '← Cardápio'),
    el('h1', {}, dish.name)
  );
  app.appendChild(header);

  // Consultoria (se ativa) — uma linha só, tipografia consistente
  const cliente = STATE.currentCliente;
  if (cliente?.show_consultor !== false && cliente?.consultor_name) {
    const text = 'Consultoria por ' + cliente.consultor_name + (cliente.consultor_info ? ' · ' + cliente.consultor_info : '');
    app.appendChild(el('div', { class: 'consultor-credit-ficha' }, text));
  }

  // Top action bar — view toggle + exports (mesma linha, mesmo tamanho)
  const toggle = el('div', { class: 'view-toggle' },
    el('button', { 'data-view': 'trabalho' }, 'Trabalho'),
    el('button', { 'data-view': 'custo' }, 'Custo')
  );
  const exports = el('div', { class: 'export-actions' },
    el('button', { class: 'btn', title: 'Exportar PDF', onclick: () => exportFichaPDF(dish, state) }, 'PDF'),
    el('button', { class: 'btn', title: 'Exportar Excel', onclick: () => exportFichaXLSX(dish, state) }, 'Excel')
  );
  const actionBar = el('div', { class: 'ficha-action-bar' }, toggle, exports);
  app.appendChild(actionBar);

  // Resumo de custo — aparece quando view === 'custo'
  const costSummaryBar = el('div', { class: 'cost-summary-bar', style: 'display:none;' });
  app.appendChild(costSummaryBar);
  function renderCostSummary() {
    const c = dishCost(dish);
    costSummaryBar.innerHTML = '';
    if (state.view !== 'custo') { costSummaryBar.style.display = 'none'; return; }
    costSummaryBar.style.display = '';
    const metrics = [
      { label: 'Custo / porção', value: fmtBRL(c.costPerPortion), tone: 'neutral' },
      { label: 'CMV', value: c.cmv ? fmtNum(c.cmv, 1) + '%' : '—', tone: 'neutral' },
      { label: 'Markup', value: c.markup ? fmtNum(c.markup, 0) + '%' : '—', tone: 'neutral' },
      { label: 'Preço sugerido', value: fmtBRL(c.suggestedPrice), tone: 'accent' }
    ];
    metrics.forEach(m => {
      costSummaryBar.appendChild(el('div', { class: 'cost-metric cost-metric-' + m.tone },
        el('span', { class: 'cost-metric-label' }, m.label),
        el('strong', { class: 'cost-metric-value' }, m.value)
      ));
    });
  }

  if (dish.photos && dish.photos.length) {
    const gallery = el('div', { class: 'gallery' });
    dish.photos.forEach(p => gallery.appendChild(el('div', { class: 'photo' }, el('img', { src: p }))));
    app.appendChild(gallery);
  }

  // Scale bar — só em modo trabalho
  const scaleBar = el('div', { class: 'rend-bar dish-scale-bar' },
    el('div', { class: 'rend-info' },
      el('span', { class: 'rend-label' }, 'Rendimento original'),
      el('span', { class: 'rend-value' }, formatRendimento(finalSf?.rendimento || '—'))
    ),
    el('div', { class: 'scale-ctrl' },
      el('span', { class: 'scale-label' }, 'Produzir:'),
      (() => {
        const input = el('input', {
          type: 'text',
          inputmode: 'decimal',
          autocomplete: 'off',
          value: String(state.finalTargetQty).replace('.', ',')
        });
        input.addEventListener('input', () => {
          // Aceita vírgula ou ponto. Filtra tudo que não é dígito/separador
          const cleaned = input.value.replace(/[^\d,.]/g, '').replace(',', '.');
          const v = parseFloat(cleaned);
          if (isNaN(v) || v <= 0) return;
          state.finalTargetQty = v;
          updateBody();
        });
        return input;
      })(),
      el('span', { class: 'scale-unit' }, state.finalUnit || '')
    )
  );
  app.appendChild(scaleBar);

  // Quick-nav anchors — ordem invertida: prato final primeiro, preparações depois
  // IMPORTANTE: usa click handler com scrollIntoView em vez de href="#sf-X" porque
  // o router escuta hashchange e jogaria o user pra rota base (bug anterior).
  const quickNav = el('nav', { class: 'sf-quicknav' });
  const subFichasReversed = [...(dish.sub_fichas || [])].reverse();
  subFichasReversed.forEach((sf) => {
    const originalIdx = dish.sub_fichas.indexOf(sf);
    const isFinal = originalIdx === dish.sub_fichas.length - 1;
    const link = el('a', { href: '#', class: isFinal ? 'is-final' : '' },
      `${originalIdx + 1}. ${sf.name}`
    );
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const target = document.getElementById(`sf-${sf.id}`);
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    quickNav.appendChild(link);
  });
  app.appendChild(quickNav);

  // Body (all sub-fichas stacked — ordem invertida no modo trabalho)
  const body = el('div', { id: 'ficha-body-container' });
  app.appendChild(body);

  function updateBody() {
    $$('.view-toggle button', toggle).forEach(b => b.classList.toggle('active', b.dataset.view === state.view));
    scaleBar.style.display = state.view === 'trabalho' ? '' : 'none';
    renderCostSummary();
    body.innerHTML = '';
    if (state.view === 'trabalho') {
      // Ordem invertida: prato final primeiro, depois preparações
      [...(dish.sub_fichas || [])].reverse().forEach((sf) => {
        const originalIdx = dish.sub_fichas.indexOf(sf);
        const card = renderFichaTrabalho(dish, sf, state, originalIdx);
        card.id = `sf-${sf.id}`;
        body.appendChild(card);
      });
      // Louça + equipamentos ao final (só uma vez)
      if (dish.louca) {
        body.appendChild(el('div', { class: 'info-box' },
          el('strong', {}, 'Apresentação / Louça'),
          dish.louca
        ));
      }
      if (dish.equipamentos && dish.equipamentos.length) {
        const box = el('div', { class: 'info-box' }, el('strong', {}, 'Equipamentos necessários'));
        const list = el('div', { class: 'equipamentos-list' });
        dish.equipamentos.forEach(eq => list.appendChild(el('span', { class: 'chip' }, eq)));
        box.appendChild(list);
        body.appendChild(box);
      }
    } else {
      body.appendChild(renderFichaCusto(dish, cid));
    }
  }
  toggle.addEventListener('click', e => { if (e.target.dataset.view) { state.view = e.target.dataset.view; updateBody(); } });
  updateBody();

  // Scroll pra sub-ficha específica SE vier do cardápio de uma sub-ficha que NÃO é a final
  // (a final já é renderizada primeiro na ordem invertida; scroll não é necessário)
  const finalId = dish.sub_fichas[dish.sub_fichas.length - 1]?.id;
  if (initialSfId && initialSfId !== finalId) {
    setTimeout(() => {
      const target = document.getElementById(`sf-${initialSfId}`);
      if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 100);
  }
}

function renderFichaTrabalho(dish, sf, state, idx) {
  const isFinal = idx === dish.sub_fichas.length - 1;
  const wrap = el('section', { class: 'sf-card kitchen-mode' + (isFinal ? ' sf-final' : '') });

  const scales = state.scales();
  const sfScale = scales[sf.id] || 1;

  // Header da sub-ficha com número + nome + rendimento
  const parsedRend = getSfRendimento(sf);
  const scaledRendQty = parsedRend.qty * sfScale;
  const normRend = normUnitForDisplay(scaledRendQty, parsedRend.unit);

  wrap.appendChild(el('header', { class: 'sf-card-head' },
    el('div', { class: 'sf-num-name' },
      el('span', { class: 'sf-num' }, String(idx + 1).padStart(2, '0')),
      el('h2', { class: 'sf-title' }, sf.name),
      isFinal ? el('span', { class: 'sf-final-tag' }, 'prato final') : null
    ),
    el('div', { class: 'sf-rend-inline' },
      el('span', { class: 'muted' }, 'rendimento '),
      el('strong', {}, `${normRend.text} ${normRend.unit}`.trim()),
      sfScale !== 1 ? el('span', { class: 'muted', style: 'margin-left:0.4rem;' }, `(${fmtNum(sfScale, 2)}×)`) : null
    )
  ));

  // Info de subproduto (se existe)
  if (sf.subproduto && sf.subproduto.name && sf.subproduto.rendimento_qty > 0) {
    const sq = sf.subproduto.rendimento_qty * sfScale;
    const sn = normUnitForDisplay(sq, sf.subproduto.rendimento_unit || '');
    wrap.appendChild(el('div', { class: 'subproduto-info' },
      el('span', { class: 'muted' }, '↳ também gera: '),
      el('strong', {}, sf.subproduto.name),
      el('span', { class: 'muted' }, ` — ${sn.text} ${sn.unit} `.trimEnd()),
      el('em', { class: 'subproduto-note' }, '(custo alocado no produto principal)')
    ));
  }

  // Ingredientes
  const sfIdx = idx;
  const ingList = el('ul', { class: 'kitchen-ingredients' });
  (sf.ingredientes || []).forEach(ing => {
    let subSf = null;
    if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
    if (!subSf && sfIdx > 0) subSf = detectSubref(dish, sfIdx, ing.insumo_name);

    const formatted = formatIngQty(ing, sfScale);
    const insumoLookup = findInsumo(ing.insumo_id);
    const isReutilizavel = !!(insumoLookup && insumoLookup.reutilizavel);
    // Detecta se o ingrediente é um subproduto de outra sub-ficha (do mesmo prato ou cross-dish)
    const subprodHit = !subSf && !isReutilizavel ? findSubproduto(ing.insumo_name) : null;
    const isSubproduto = !!(subprodHit && !(subprodHit.dish.id === dish.id && subprodHit.sf.id === sf.id));
    const displayName = (subSf ? '↪ ' : '') + (isSubproduto ? '↳ ' : '') + ing.insumo_name + (ing.variation_name ? ` — ${ing.variation_name}` : '');
    const li = el('li', { class: 'kitchen-ing' + (subSf ? ' is-subref' : '') + (isReutilizavel ? ' is-reut' : '') + (isSubproduto ? ' is-subproduto' : '') },
      el('div', { class: 'k-row-main' },
        el('span', { class: 'k-name' }, displayName,
          isReutilizavel ? el('span', { class: 'reut-badge', title: 'Reutilizável — não entra no custo do prato' }, 'rateio') : null,
          isSubproduto ? el('span', { class: 'subproduto-badge', title: `Subproduto de "${subprodHit.sf.name}" (${subprodHit.dish.name}) — custo alocado lá` }, 'subproduto') : null
        ),
        el('span', { class: 'k-qty' },
          el('span', { class: 'qty-num' }, formatted.text),
          formatted.unit ? el('span', { class: 'qty-unit' }, formatted.unit) : null
        )
      ),
      ing.observacao ? el('div', { class: 'k-obs' }, ing.observacao) : null,
      subSf ? el('div', { class: 'k-sub-note' }, el('a', { href: `#sf-${subSf.id}` }, 'ver preparação: ' + subSf.name)) : null,
      isSubproduto && subprodHit.dish.id !== dish.id ? el('div', { class: 'k-sub-note' },
        el('a', { href: `#/c/${STATE.currentClienteId}/ficha/${subprodHit.dish.id}` }, `produzido em: ${subprodHit.dish.name} / ${subprodHit.sf.name}`)
      ) : (isSubproduto ? el('div', { class: 'k-sub-note' }, el('a', { href: `#sf-${subprodHit.sf.id}` }, `produzido em: ${subprodHit.sf.name}`)) : null)
    );
    ingList.appendChild(li);
  });

  // Alerta de superconsumo de subproduto dentro do mesmo prato
  if (sf.subproduto && sf.subproduto.name && sf.subproduto.rendimento_qty > 0) {
    const consumption = computeSubprodConsumption(dish, sf.subproduto.name, sf.subproduto.rendimento_unit);
    const produced = sf.subproduto.rendimento_qty;
    if (consumption > produced * 1.001) {
      wrap.appendChild(el('div', { class: 'subprod-alert' },
        `⚠ Consumo interno de "${sf.subproduto.name}" (${fmtNum(consumption, 2)} ${sf.subproduto.rendimento_unit || ''}) excede o rendimento (${fmtNum(produced, 2)} ${sf.subproduto.rendimento_unit || ''}). Ajuste a receita mestre pra produzir o mínimo necessário.`
      ));
    }
  }
  wrap.appendChild(el('h3', { class: 'k-section-title' }, 'Ingredientes'));
  wrap.appendChild(ingList);

  if (sf.modo_preparo) {
    wrap.appendChild(el('h3', { class: 'k-section-title' }, 'Modo de preparo'));
    wrap.appendChild(el('div', { class: 'kitchen-preparo' }, sf.modo_preparo));
  }

  return wrap;
}

function renderFichaCusto(dish, cid) {
  const wrap = el('div', { class: 'ficha-body' });
  if (!canEditInsumoPrice(cid)) {
    wrap.appendChild(el('p', { class: 'muted' }, 'Ficha de custo não disponível para seu perfil.'));
    return wrap;
  }

  const all = dishCost(dish);
  const finalSfId = dish.sub_fichas[dish.sub_fichas.length - 1]?.id;
  all.sfCosts.forEach(({ sf, rows, total }, idx) => {
    const isFinal = sf.id === finalSfId;
    const rendDisplay = sf.rendimento ? formatRendimento(sf.rendimento) : '';
    const section = el('section', { class: 'sf-cost-card' + (isFinal ? ' sf-final' : '') , id: `sf-cost-${sf.id}` },
      el('header', { class: 'sf-cost-head' },
        el('div', {},
          el('span', { class: 'sf-num' }, String(idx + 1).padStart(2, '0')),
          el('h3', { class: 'sf-cost-title' }, sf.name),
          isFinal ? el('span', { class: 'sf-final-tag' }, 'prato final') : null
        ),
        el('span', { class: 'muted' }, rendDisplay ? 'rend. ' + rendDisplay : '')
      )
    );
    if (sf.subproduto && sf.subproduto.name && sf.subproduto.rendimento_qty > 0) {
      const sn = normUnitForDisplay(sf.subproduto.rendimento_qty, sf.subproduto.rendimento_unit || '');
      section.appendChild(el('div', { class: 'subproduto-info' },
        el('span', { class: 'muted' }, '↳ também gera: '),
        el('strong', {}, sf.subproduto.name),
        el('span', { class: 'muted' }, ` — ${sn.text} ${sn.unit} `.trimEnd()),
        el('em', { class: 'subproduto-note' }, '(custo R$ 0,00 — alocado aqui)')
      ));
    }
    const tbl = el('table', { class: 'ficha-table' },
      el('thead', {}, el('tr', {},
        el('th', {}, 'Insumo'),
        el('th', { class: 'num' }, 'Qtd'),
        el('th', {}, 'Un.'),
        el('th', { class: 'num' }, 'Preço unit.'),
        el('th', { class: 'num' }, 'Custo')
      )),
      el('tbody', {}, ...rows.map(({ ing, insumo, cost, isSubref, subSf, fc, isReutilizavel, isSubproduto, subprodHit }) => {
        const fmt = formatIngQty(ing);
        let priceTxt, nameCell, costCell;
        if (isSubref && subSf) {
          priceTxt = el('span', { class: 'subref-note' }, `sub-ficha`);
          nameCell = el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↪ '), ing.insumo_name);
          costCell = el('td', { class: 'num', 'data-label': 'Custo' }, fmtBRL(cost));
        } else if (isSubproduto) {
          const title = `Subproduto de "${subprodHit.sf.name}" (${subprodHit.dish.name}) — custo alocado lá`;
          priceTxt = el('span', { class: 'subref-note', title }, 'subproduto');
          nameCell = el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↳ '), ing.insumo_name,
            el('span', { class: 'subproduto-badge', title }, 'subproduto'));
          costCell = el('td', { class: 'num reut-cost', 'data-label': 'Custo', title: 'Subproduto — custo alocado no produto principal' }, 'R$ 0,00');
        } else if (isReutilizavel) {
          const normPrice = insumo ? normalizePriceForDisplay(insumo) : null;
          priceTxt = normPrice ? `${fmtBRL(normPrice.price)} / ${normPrice.unit}` : '—';
          const reutBadge = el('span', { class: 'reut-badge', title: 'Insumo reutilizável — lançar em despesas operacionais, não em CMV' }, 'rateio');
          nameCell = el('td', { 'data-label': 'Insumo' }, ing.insumo_name, ' ', reutBadge);
          costCell = el('td', { class: 'num reut-cost', 'data-label': 'Custo', title: 'Não entra no custo do prato. Lançar em despesas operacionais.' }, 'R$ 0,00');
        } else {
          const normPrice = insumo ? normalizePriceForDisplay(insumo) : null;
          priceTxt = normPrice ? `${fmtBRL(normPrice.price)} / ${normPrice.unit}` : '—';
          const fcBadge = (fc && fc > 1) ? el('span', { class: 'fc-badge', title: `Fator de correção ${fc}x — considera perda no processamento` }, ` · FC ${fmtNum(fc, 2)}`) : null;
          const varBadge = ing.variation_name ? el('span', { class: 'var-badge', title: 'Variação do insumo' }, ` — ${ing.variation_name}`) : null;
          nameCell = el('td', { 'data-label': 'Insumo' }, ing.insumo_name, varBadge, fcBadge);
          costCell = el('td', { class: 'num', 'data-label': 'Custo' }, fmtBRL(cost));
        }
        return el('tr', { class: (isSubref ? 'row-subref' : '') + (isReutilizavel ? ' row-reut' : '') + (isSubproduto ? ' row-subproduto' : '') },
          nameCell,
          el('td', { class: 'num', 'data-label': 'Qtd' }, fmt.text),
          el('td', { 'data-label': 'Un.' }, fmt.unit || '—'),
          el('td', { class: 'num', 'data-label': 'Preço unit.' }, priceTxt),
          costCell
        );
      })),
      el('tfoot', {}, el('tr', {},
        el('td', { colspan: '4' }, isFinal ? 'Subtotal (= custo do prato)' : 'Subtotal do lote (informativo)'),
        el('td', { class: 'num' }, fmtBRL(total))
      ))
    );
    section.appendChild(tbl);
    wrap.appendChild(section);
  });

  // Display principal: custo por porção (destaque).
  // Se rendimento > 1, mostra linha discreta com total e quantidade de porções.
  const total = el('div', { class: 'total-dish-cost' },
    el('div', {}, el('span', { class: 'stat-label' }, 'Custo por porção'),
      el('span', { class: 'stat-value gold' }, fmtBRL(all.costPerPortion)))
  );
  wrap.appendChild(total);
  if (all.portions > 1) {
    wrap.appendChild(el('p', { class: 'cost-total-note' },
      `Produz ${fmtNum(all.portions, 0)} porções · total R$ ${fmtNum(all.total, 2)}`));
  }

  // 3 campos bidirecionais: CMV / Markup / Preço de Venda
  // Edite qualquer um → os outros 2 se ajustam. Armazena target_cmv como fonte de verdade.
  const costBox = el('div', { class: 'cost-summary' });
  const costPP = all.costPerPortion;
  const initialCmv = dish.target_cmv || 30;
  const initialPrice = costPP > 0 ? costPP / (initialCmv / 100) : 0;
  const initialMarkup = costPP > 0 ? ((initialPrice / costPP) - 1) * 100 : 0;

  const editable = canEditInsumoPrice(cid);
  const parseDec = (s) => {
    if (s == null) return NaN;
    const v = String(s).replace(/\./g, '').replace(',', '.').trim();
    return parseFloat(v);
  };
  const cmvInput = el('input', { type: 'text', inputmode: 'decimal', value: initialCmv.toFixed(1).replace('.', ',') });
  const markupInput = el('input', { type: 'text', inputmode: 'decimal', value: initialMarkup.toFixed(0) });
  const priceInput = el('input', { type: 'text', inputmode: 'decimal', value: initialPrice.toFixed(2).replace('.', ',') });
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
    const cmv = parseDec(cmvInput.value);
    if (isNaN(cmv) || cmv <= 0) return;
    dish.target_cmv = cmv;
    const price = costPP / (cmv / 100);
    const markup = ((price / costPP) - 1) * 100;
    internalUpdate = true;
    priceInput.value = price.toFixed(2).replace('.', ',');
    markupInput.value = markup.toFixed(0);
    internalUpdate = false;
    save();
  });
  markupInput.addEventListener('input', () => {
    if (internalUpdate) return;
    const markup = parseDec(markupInput.value);
    if (isNaN(markup) || markup < 0) return;
    const price = costPP * (1 + markup / 100);
    const cmv = price > 0 ? (costPP / price) * 100 : 0;
    dish.target_cmv = cmv;
    internalUpdate = true;
    cmvInput.value = cmv.toFixed(1).replace('.', ',');
    priceInput.value = price.toFixed(2).replace('.', ',');
    internalUpdate = false;
    save();
  });
  priceInput.addEventListener('input', () => {
    if (internalUpdate) return;
    const price = parseDec(priceInput.value);
    if (isNaN(price) || price <= 0) return;
    const cmv = (costPP / price) * 100;
    const markup = ((price / costPP) - 1) * 100;
    dish.target_cmv = cmv;
    internalUpdate = true;
    cmvInput.value = cmv.toFixed(1).replace('.', ',');
    markupInput.value = markup.toFixed(0);
    internalUpdate = false;
    save();
  });

  return wrap;
}

// ---------- Views: Insumos ----------
// Auto-categorização por palavras-chave no nome
const INSUMO_CATEGORIES = [
  { id: 'proteinas', label: 'Proteínas', kws: ['acem','atum','barriga','copa','coracao','costelinha','frango','joelho','linguica','lula','moela','osso','ovo','peito','peixe','pele','pe de','rabada','gema','coracao','suina','suino'] },
  { id: 'hortifruti', label: 'Hortifruti', kws: ['agriao','alho','banana','cebola','cebolinha','cenoura','ciboulette','coentro','dill','gengibre','limao','maca','milho','salsinha','salsao','tomate','quirera'] },
  { id: 'laticinios', label: 'Laticínios', kws: ['leite','manteiga','mussarela','queijo','requeijao','catupiry','coco'] },
  { id: 'secos', label: 'Secos / Especiarias', kws: ['acucar','amido','cominho','cravo','extrato','farinha','flor de sal','fuba','louro','mostarda','panko','paprica','pimenta','polvilho','sal ','sal refinado','sal\u00a0'] },
  { id: 'liquidos', label: 'Líquidos / Condimentos', kws: ['agua','azeite','cerveja','molho','oleo','shoyu','suco','sweet','vinagre','chili'] },
];
function categorizeInsumo(ins) {
  const n = (ins.name || '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
  for (const cat of INSUMO_CATEGORIES) {
    for (const kw of cat.kws) {
      if (n.includes(kw)) return cat.id;
    }
  }
  return 'outros';
}

// ---------- Views: Produção ----------
// Estado do plano de produção (sessão apenas, não persistido)
const PROD_PLAN = { items: [] };
// item: { dishId, targetQty, targetUnit, excludedSfIds: Set<string> }

// ============================================================================
// ============================ MÓDULO DE ESTOQUE =============================
// ============================================================================
// Estrutura de dados:
// template = { id, type: 'stock_count', name, active, sections: [{ id, name, group, team, columns: ['Pedido','Contagem Bar','Contagem Estoque'], items: [{ id, name, unit }] }] }
// count = { id, templateId, templateName, date, authorUid, authorEmail, authorName, status: 'sent'|'draft', values: { [sectionId]: { [itemId]: { [colKey]: value } } }, obsAdicional, createdAt, updatedAt }
// log = { id, timestamp, user: {uid, email, name}, action: 'submit'|'edit'|'reopen', note }

// Seed template do Bar Sororoca
const SOROROCA_TEMPLATE = {
  type: 'stock_count',
  name: 'Pedidos Bar Sororoca',
  active: true,
  description: 'Contagem de vinhos + insumos do bar',
  sections: [
    // Vinhos — só contagem
    { id: 's-barrinhas', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Barrinhas', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'boas-quintas-morgado', name: 'Boas Quintas, Morgado de Bucelas', unit: 'und' }]},
    { id: 's-berkeman', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Berkeman', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'matias-riccitelli-kung', name: 'Matias Riccitelli, Kung Fu Pet Nat', unit: 'und' }]},
    { id: 's-diasa', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Diasa', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'vina-alta-esencial', name: 'Viña Alta, Esencial Naranjo', unit: 'und' }]},
    { id: 's-europa', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Europa', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'domaine-les-malandes', name: 'Domaine Les Malandes, Chablis', unit: 'und' },
      { id: 'barbadillo-tamarix', name: 'Barbadillo, Tamarix', unit: 'und' },
      { id: 'andi-weigand-white', name: 'Andi Weigand, White', unit: 'und' }]},
    { id: 's-grapy', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Grapy', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'castelo-dalba-branco', name: "Castelo D'Alba Branco, Rui Roboredo Madeira", unit: 'und' },
      { id: 'els-nanos-blanc', name: 'Els Nanos Blanc del Coster, Joseph Foraster', unit: 'und' },
      { id: 'les-gallinetes-tinto', name: 'Les Gallinetes Tinto, Joseph Foraster', unit: 'und' }]},
    { id: 's-tanyno', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Tanyno', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'calcarius-chablis', name: 'Calcarius Chablis', unit: 'und' }]},
    { id: 's-uva', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Uva', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'meinklang-burgenland', name: 'Meinklang, Burgenland Weiss', unit: 'und' }]},
    { id: 's-vm', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos VM', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'arniston-bay', name: 'Arniston Bay', unit: 'und' },
      { id: 'busy-bee', name: 'Busy Bee', unit: 'und' },
      { id: 'chenin-les-athletes', name: 'Chenin Les Athletes du Vin', unit: 'und' },
      { id: 'heiderer-meyer', name: 'Heiderer Meyer', unit: 'und' },
      { id: 'henri-kieffer-riesling', name: 'Henri Kieffer & Fils, Riesling', unit: 'und' }]},
    { id: 's-world-wine', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos World Wine', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'gerard-berthrand-papilou', name: 'Gerard Berthrand, Papilou', unit: 'und' },
      { id: 'portal-calcada-patusco', name: 'Portal da Calçada, Patusco', unit: 'und' }]},
    { id: 's-zahil', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Vinhos Zahil', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'la-guita-manzanilla', name: 'La Guita Manzanilla', unit: 'und' }]},
    { id: 's-mega-sake', group: 'Pedidos Vinhos Sororoca', team: 'Equipe do Salão', name: 'Mega Sakê', columns: ['Contagem Adega', 'Contagem Estoque'], items: [
      { id: 'sake-niida-shizenshu', name: 'Sake Niida Shizenshu Kan Atsurae Kimoto Junmai', unit: 'und' }]},
    // Insumos do bar — pedido + contagem
    { id: 's-agro-bonfim', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Agro Bonfim', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'amora', name: 'Amora', unit: 'bandeja' },
      { id: 'caju', name: 'Caju', unit: 'bandeja' },
      { id: 'jabuticaba', name: 'Jabuticaba', unit: 'kg' },
      { id: 'kiwi', name: 'Kiwi', unit: 'und' },
      { id: 'laranja-bahia', name: 'Laranja Bahia', unit: 'kg' },
      { id: 'limao-cravo', name: 'Limão Cravo', unit: 'kg' },
      { id: 'limao-siciliano', name: 'Limão Siciliano', unit: 'kg' },
      { id: 'limao-tahiti', name: 'Limão Tahiti', unit: 'kg' },
      { id: 'manga', name: 'Manga', unit: 'und' },
      { id: 'maracuja', name: 'Maracujá', unit: 'kg' },
      { id: 'melao', name: 'Melão', unit: 'und' },
      { id: 'seriguela-bonfim', name: 'Seriguela', unit: 'kg' },
      { id: 'tangerina', name: 'Tangerina', unit: 'kg' }]},
    { id: 's-alibec', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Alibec', columns: ['Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'amendoim-torrado', name: 'Amendoim torrado sem sal e sem pele', unit: 'und' }]},
    { id: 's-matury', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Matury', columns: ['Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'cajuina-1l', name: 'Cajuina orgânica 1L', unit: 'und' },
      { id: 'cajuina-310', name: 'Cajuina orgânica 310ml', unit: 'und' }]},
    { id: 's-singlefin', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Singlefin', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'bergamoncello', name: 'Bergamoncello', unit: 'und' },
      { id: 'singlefin-gin', name: 'Singlefin Gin', unit: 'und' },
      { id: 'gin-ocean', name: 'Gin Ocean', unit: 'und' },
      { id: 'gin-refil', name: 'Gin Refil', unit: 'und' }]},
    { id: 's-fg7', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'FG7 Bebidas', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'aperol', name: 'Aperol', unit: 'und' },
      { id: 'bitter-angostura', name: 'Bitter Angostura', unit: 'und' },
      { id: 'campari', name: 'Campari', unit: 'und' },
      { id: 'cinzano-rosso', name: 'Cinzano Rosso', unit: 'und' },
      { id: 'licor-43', name: 'Licor 43 Diego Zamora 750ml', unit: 'und' },
      { id: 'licor-cointreau', name: 'Licor Cointreau 700ml', unit: 'und' },
      { id: 'licor-tia-maria', name: 'Licor de Café Tia Maria 750ml', unit: 'und' },
      { id: 'licor-frangelico', name: 'Licor Frangelico 750ml', unit: 'und' },
      { id: 'noilly-prat', name: 'Noilly Prat', unit: 'und' },
      { id: 'rum-havana-3', name: 'Rum Havana 3 750ml', unit: 'und' },
      { id: 'rum-havana-7', name: 'Rum Havana 7 750ml', unit: 'und' },
      { id: 'salton-brut', name: 'Salton Brut', unit: 'und' },
      { id: 'suco-tomate-raiola', name: 'Suco de Tomate Raiola 1L', unit: 'und' },
      { id: 'tequila-jimador', name: 'Tequila El Jimador branca 750ml', unit: 'und' },
      { id: 'vodka-absolut', name: 'Vodka Absolut 1L', unit: 'und' },
      { id: 'vodka-smirnoff', name: 'Vodka Smirnoff 998ml', unit: 'und' },
      { id: 'whisky-dewars', name: "Whisky Dewar's", unit: 'und' },
      { id: 'whisky-glenlivet', name: 'Whisky Glenlivet', unit: 'und' },
      { id: 'whisky-jack-daniels', name: "Whisky Jack Daniel's 1L", unit: 'und' },
      { id: 'whisky-jim-beam', name: 'Whisky Jim Beam', unit: 'und' },
      { id: 'whisky-makers-mark', name: "Whisky Maker's Mark", unit: 'und' },
      { id: 'whisky-old-parr', name: 'Whisky Old Parr', unit: 'und' }]},
    { id: 's-heineken', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Heineken', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'agua-tonica-vys', name: 'Água tônica Vys', unit: 'und' },
      { id: 'agua-tonica-vys-zero', name: 'Água tônica Vys zero', unit: 'und' },
      { id: 'baer-mate', name: 'Baer mate', unit: 'und' },
      { id: 'agua-sem-gas-mamba', name: 'Água sem gás Mamba', unit: 'und' },
      { id: 'agua-com-gas-mamba', name: 'Água com gás Mamba', unit: 'und' },
      { id: 'heineken-ln', name: 'Heineken Long Neck', unit: 'und' },
      { id: 'heineken-zero-ln', name: 'Heineken Zero Long Neck', unit: 'und' },
      { id: 'lagunitas-ipa', name: 'Lagunitas IPA', unit: 'und' },
      { id: 'praya-classica', name: 'Praya Clássica', unit: 'und' },
      { id: 'praya-sem-gluten', name: 'Praya Lager sem Glúten', unit: 'und' }]},
    { id: 's-lobozo', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Bebidas Lobozó', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'cachaca-lobozo-branca', name: 'Cachaça Lobozó Branca', unit: 'und' },
      { id: 'cachaca-lobozo-envelhecida', name: 'Cachaça Lobozó Envelhecida', unit: 'und' }]},
    { id: 's-remanso-peixe', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Remanso do Peixe', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'cachaca-jambu-remanso', name: 'Cachaça de Jambu Remanso', unit: 'und' }]},
    { id: 's-princesa-isabel', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Princesa Isabel', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'cachaca-jiquitaia-branca', name: 'Cachaça Jiquitaia branca', unit: 'und' }]},
    { id: 's-aptk', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'APTK', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'vermouth-circollo', name: 'Vermouth Circollo', unit: 'caixa 6 und' }]},
    { id: 's-eden-coco', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Eden coco', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'agua-de-coco', name: 'Água de coco', unit: 'cx 15 und' }]},
    { id: 's-bicudo', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Bicudo', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'seriguela-bicudo', name: 'Seriguela', unit: 'kg' },
      { id: 'graviola', name: 'Graviola', unit: 'kg' },
      { id: 'cupuacu', name: 'Cupuaçu', unit: 'kg' },
      { id: 'cambuci', name: 'Cambuci', unit: 'kg' }]},
    { id: 's-escola-sorvete', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Escola do Sorvete', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'baunilha-500ml', name: 'Baunilha', unit: '500ml' }]},
    { id: 's-especiais', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Especiais', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'tabasco', name: 'Tabasco', unit: 'und' },
      { id: 'mate-tostado', name: 'Mate Tostado', unit: 'und' },
      { id: 'azeitona-asaro', name: 'Azeitona Asaro', unit: 'und' },
      { id: 'tucupi-preto', name: 'Tucupi preto', unit: 'und' },
      { id: 'flor-de-sal-cimsal', name: 'Flor de sal Cimsal', unit: 'und' }]},
    { id: 's-icy-code', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Icy Code', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'gelo-translucido', name: 'Gelo translúcido', unit: 'pacote' }]},
    { id: 's-gelo-top', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Gelo Top', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'gelo-5kg', name: 'Gelo 5 KG', unit: 'pacote' }]},
    { id: 's-combu', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Combu', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'polpa-cupuacu', name: 'Polpa de cupuaçu integral para sobremesa', unit: 'kg' },
      { id: 'polpa-bacuri', name: 'Polpa de Bacuri', unit: 'kg' }]},
    { id: 's-terra-serra', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Terra da Serra', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'molho-ingles-5l', name: 'Molho inglês 5L', unit: 'gl' }]},
    { id: 's-tocaya', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Tocaya', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'cafe-graos', name: 'Café em grãos', unit: 'kg' }]},
    { id: 's-sobremesas', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Sobremesas', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'torta-chocolate-castanha', name: 'Torta de chocolate com castanha', unit: 'und' },
      { id: 'pave-cupuacu', name: 'Pavê de Cupuaçu', unit: 'und' }]},
    { id: 's-mega-g', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Mega G', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'biscoito-champagne', name: 'Biscoito champagne', unit: 'pct' },
      { id: 'cacau-em-po', name: 'Cacau em pó', unit: 'kg' },
      { id: 'creme-de-leite', name: 'Creme de leite', unit: '200g' },
      { id: 'gelatina-em-po', name: 'Gelatina em pó incolor', unit: 'sache' },
      { id: 'leite', name: 'Leite', unit: 'l' },
      { id: 'leite-condensado', name: 'Leite condensado', unit: 'lata' },
      { id: 'po-cafe-funcionario', name: 'Pó de café funcionário', unit: 'kg' },
      { id: 'oleo-coco', name: 'Óleo de coco', unit: 'und' }]},
    { id: 's-copos', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Copos', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'americano-grande', name: 'Americano Grande', unit: 'und' },
      { id: 'americano-medio', name: 'Americano Médio', unit: 'und' },
      { id: 'americano-pequeno', name: 'Americano Pequeno', unit: 'und' },
      { id: 'americano-dose', name: 'Americano Dose', unit: 'und' },
      { id: 'rabo-de-peixe', name: 'Rabo de Peixe', unit: 'und' },
      { id: 'caipirinha', name: 'Caipirinha', unit: 'und' },
      { id: 'dry-martini', name: 'Dry Martini', unit: 'und' },
      { id: 'copo-longo-coqueiro', name: 'Copo Longo coqueiro', unit: 'und' },
      { id: 'garrafa-de-suco', name: 'Garrafa de suco', unit: 'und' },
      { id: 'copo-barriquinha', name: 'Copo Barriquinha', unit: 'und' }]},
    { id: 's-descartaveis', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Descartáveis', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'palito-caipirinha-18', name: 'Palito caipirinha golf 18cm', unit: 'pct 50un' },
      { id: 'palito-batidinha-15', name: 'Palito batidinha golf 15cm', unit: 'pct 50un' },
      { id: 'tampa-garrafa-suco', name: 'Tampa garrafa de suco alumínio long neck 27mm dourada', unit: '100 und' },
      { id: 'copo-descartavel-300', name: 'Copo descartável com tampa 300ml papel', unit: 'pct' }]},
    { id: 's-utensilios', group: 'Pedidos Insumos Bar Sororoca', team: 'Equipe do Bar', name: 'Utensílios', columns: ['Pedido', 'Contagem Bar', 'Contagem Estoque'], items: [
      { id: 'medidor-vinho', name: 'Medidor de Vinho', unit: 'und' },
      { id: 'peneira-pequena', name: 'Peneira Pequena', unit: 'und' },
      { id: 'abridor-vinho', name: 'Abridor de vinho', unit: 'und' },
      { id: 'palito-drink-inox', name: 'Palito para drink em inox', unit: 'und' }]},
  ]
};

async function seedSororocaTemplate(cid) {
  const ref = doc(stockTemplatesCol(cid), 'sororoca-bar-v1');
  const existing = await getDoc(ref);
  if (existing.exists()) { toast('Template já existe'); return; }
  await setDoc(ref, { ...SOROROCA_TEMPLATE, createdAt: serverTimestamp() });
  toast('Template Sororoca criado');
}

// Chave única pra cada campo da contagem
function valueKey(sectionId, itemId, colIdx) {
  return `${sectionId}::${itemId}::${colIdx}`;
}

// Cache em memória dos templates e contagens do cliente atual
const STOCK_STATE = { templates: [], counts: [], currentCid: null };

async function loadStockData(cid) {
  if (STOCK_STATE.currentCid !== cid) {
    STOCK_STATE.templates = [];
    STOCK_STATE.counts = [];
    STOCK_STATE.currentCid = cid;
  }
  // Templates: todos com acesso ao cliente podem ler
  const tSnap = await getDocs(stockTemplatesCol(cid));
  STOCK_STATE.templates = tSnap.docs.map(d => ({ id: d.id, ...d.data() }));
  // Counts: equipe sem can_manage_contagem só lê as próprias (filtro por authorUid)
  try {
    let cSnap;
    if (isEquipe() && !equipePerm('can_manage_contagem')) {
      cSnap = await getDocs(query(stockCountsCol(cid), where('authorUid', '==', STATE.user.uid)));
    } else {
      cSnap = await getDocs(stockCountsCol(cid));
    }
    STOCK_STATE.counts = cSnap.docs.map(d => ({ id: d.id, ...d.data() }))
      .sort((a, b) => {
        const aT = a.createdAt?.toMillis?.() || a.createdAt || 0;
        const bT = b.createdAt?.toMillis?.() || b.createdAt || 0;
        return bT - aT;
      });
  } catch (err) {
    console.warn('stock counts load:', err);
    STOCK_STATE.counts = [];
  }
}

// =============== ROUTER ESTOQUE ===============
async function renderEstoque(cid, subroute) {
  const app = $('#app');
  renderLoadingScreen();
  try {
    await loadStockData(cid);
  } catch (err) {
    console.error('loadStockData failed:', err);
    app.innerHTML = '';
    app.appendChild(renderClienteContext(cid));
    app.appendChild(el('div', { class: 'empty-state' },
      el('h2', {}, 'Erro ao carregar estoque'),
      el('p', { class: 'muted' }, err.message || String(err)),
      el('p', {}, 'Provável causa: regras do Firestore ainda não foram publicadas. Publique as novas regras no console Firebase (Firestore → Rules) e tente de novo.')
    ));
    return;
  }
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));

  const sub = subroute[0] || '';
  // Sub-aba no header
  const subNav = el('nav', { class: 'estoque-subnav' },
    el('a', { class: 'estoque-subtab' + (!sub ? ' active' : ''), href: `#/c/${cid}/estoque` }, '⚬ Preencher contagem'),
    canManageStock(cid) ? el('a', { class: 'estoque-subtab' + (sub === 'contagens' ? ' active' : ''), href: `#/c/${cid}/estoque/contagens` }, '◨ Contagens recebidas') : null,
    canManageTemplates(cid) ? el('a', { class: 'estoque-subtab' + (sub === 'templates' ? ' active' : ''), href: `#/c/${cid}/estoque/templates` }, '✎ Templates') : null,
  );
  app.appendChild(subNav);

  if (!sub) { renderStockTemplatesList(cid, app); return; }
  if (sub === 'preencher' && subroute[1]) { renderStockCountForm(cid, subroute[1], subroute[2], app); return; }
  if (sub === 'contagens') {
    if (!canManageStock(cid)) { app.appendChild(renderNoAccess()); return; }
    if (subroute[1]) return renderStockCountDetail(cid, subroute[1], app);
    return renderStockCountsList(cid, app);
  }
  if (sub === 'templates') {
    if (!canManageTemplates(cid)) { app.appendChild(renderNoAccess()); return; }
    if (subroute[1] === 'novo') return renderStockTemplateEdit(cid, null, app);
    if (subroute[1] === 'editar' && subroute[2]) return renderStockTemplateEdit(cid, subroute[2], app);
    return renderStockTemplatesAdminList(cid, app);
  }
  app.appendChild(el('p', { class: 'muted' }, 'Rota inválida'));
}

// =============== FRONT: lista de templates pra preencher ===============
function renderStockTemplatesList(cid, app) {
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Preencher contagem'),
      el('p', {}, 'Escolha um template pra começar a contagem. Salva automaticamente enquanto você preenche.')
    )
  ));
  const activeTemplates = STOCK_STATE.templates.filter(t => t.active !== false);
  if (activeTemplates.length === 0) {
    const empty = el('div', { class: 'empty-state' },
      el('p', {}, 'Nenhum template de contagem cadastrado.'),
    );
    if (canManageTemplates(cid)) {
      empty.appendChild(el('a', { class: 'btn btn-primary', href: `#/c/${cid}/estoque/templates/novo` }, '+ Criar template'));
      empty.appendChild(el('button', { class: 'btn', onclick: async () => {
        try { await seedSororocaTemplate(cid); renderEstoque(cid, []); } catch (e) { toast('Erro: ' + e.message); }
      } }, '↻ Seed Sororoca'));
    }
    app.appendChild(empty);
    return;
  }
  const grid = el('div', { class: 'template-cards-grid' });
  activeTemplates.forEach(t => {
    const itemCount = (t.sections || []).reduce((s, sec) => s + (sec.items?.length || 0), 0);
    grid.appendChild(el('a', { class: 'template-card', href: `#/c/${cid}/estoque/preencher/${t.id}` },
      el('h3', {}, t.name),
      el('p', { class: 'muted' }, t.description || `${(t.sections || []).length} seções · ${itemCount} itens`),
      el('span', { class: 'template-card-action' }, 'Iniciar contagem →')
    ));
  });
  app.appendChild(grid);
}

// =============== FRONT: formulário de preenchimento ===============
// Extrai lista de localizações únicas a partir das colunas "Contagem X" de todas as seções
// Retorna [{ label: 'Bar', colName: 'Contagem Bar' }, { label: 'Estoque', colName: 'Contagem Estoque' }, ...]
function extractLocations(template) {
  const seen = new Map();
  (template.sections || []).forEach(section => {
    (section.columns || []).forEach(col => {
      const c = String(col).trim();
      const lower = c.toLowerCase();
      if (lower.startsWith('contagem')) {
        const label = c.replace(/^contagem\s*/i, '').trim() || 'Contagem';
        if (!seen.has(label)) seen.set(label, { label, colName: c });
      }
    });
  });
  return [...seen.values()];
}

// Sorteia itens pra uma localização específica: retorna lista de { section, item, colIdx } onde a seção tem essa coluna
function itemsForLocation(template, location) {
  const out = [];
  (template.sections || []).forEach(section => {
    const colIdx = (section.columns || []).findIndex(c => String(c).trim().toLowerCase() === location.colName.toLowerCase());
    if (colIdx < 0) return;
    (section.items || []).forEach(item => {
      out.push({ section, item, colIdx });
    });
  });
  return out;
}

async function renderStockCountForm(cid, templateId, resumeCountId, app) {
  const template = STOCK_STATE.templates.find(t => t.id === templateId);
  if (!template) { app.appendChild(el('p', {}, 'Template não encontrado')); return; }

  // Estado local: values + obs
  const lsKey = `stock-draft-${cid}-${templateId}`;
  let state;
  if (resumeCountId) {
    const countDoc = STOCK_STATE.counts.find(c => c.id === resumeCountId);
    state = { values: countDoc?.values || {}, obsAdicional: countDoc?.obsAdicional || '', currentLocation: null };
  } else {
    const saved = localStorage.getItem(lsKey);
    state = saved ? JSON.parse(saved) : { values: {}, obsAdicional: '', currentLocation: null };
  }
  const persist = () => {
    if (!resumeCountId) localStorage.setItem(lsKey, JSON.stringify(state));
  };

  const locations = extractLocations(template);
  // Contagens de preenchimento por localização
  function locProgress(loc) {
    const items = itemsForLocation(template, loc);
    let filled = 0;
    items.forEach(({ section, item, colIdx }) => {
      const v = state.values?.[section.id]?.[item.id]?.[colIdx];
      if (v !== '' && v != null) filled++;
    });
    return { filled, total: items.length };
  }
  function totalProgress() {
    let f = 0, t = 0;
    locations.forEach(loc => { const p = locProgress(loc); f += p.filled; t += p.total; });
    return { filled: f, total: t };
  }

  function render() {
    app.innerHTML = '';
    app.appendChild(renderClienteContext(cid));
    app.appendChild(el('nav', { class: 'estoque-subnav' },
      el('a', { class: 'estoque-subtab active', href: `#/c/${cid}/estoque` }, '⚬ Preencher contagem'),
    ));
    app.appendChild(el('a', { href: `#/c/${cid}/estoque`, class: 'back-link' }, '← Voltar'));

    const tp = totalProgress();
    // Header com nome e progresso
    app.appendChild(el('div', { class: 'count-header' },
      el('h1', {}, template.name),
      el('p', { class: 'muted' }, resumeCountId ? 'Editando contagem enviada' : 'Rascunho salvo automaticamente no dispositivo'),
      el('div', { class: 'progress-bar' },
        (() => { const f = el('div', { class: 'progress-fill' }); f.style.width = (tp.total ? (tp.filled / tp.total * 100) : 0) + '%'; return f; })(),
        el('span', { class: 'progress-text' }, `${tp.filled} de ${tp.total} itens preenchidos`))
    ));

    if (!state.currentLocation) {
      // Tela de seleção de localização
      app.appendChild(el('h2', { class: 'count-step-title' }, 'Onde você vai contar agora?'));
      const locGrid = el('div', { class: 'location-grid' });
      locations.forEach(loc => {
        const p = locProgress(loc);
        const pct = p.total > 0 ? (p.filled / p.total) * 100 : 0;
        const done = pct === 100;
        const started = p.filled > 0 && pct < 100;
        const card = el('button', { class: 'location-card' + (done ? ' done' : '') + (started ? ' started' : ''), onclick: () => {
          state.currentLocation = loc.label; persist(); render();
        } });
        card.appendChild(el('div', { class: 'location-icon' }, locationIcon(loc.label)));
        card.appendChild(el('div', { class: 'location-name' }, loc.label));
        card.appendChild(el('div', { class: 'location-progress' }, `${p.filled} / ${p.total}`));
        if (done) card.appendChild(el('span', { class: 'location-done-check' }, '✓'));
        locGrid.appendChild(card);
      });
      app.appendChild(locGrid);

      // Observação final (só aparece aqui)
      app.appendChild(el('div', { class: 'stock-obs-wrap' },
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Observações finais (opcional)'),
          (() => { const ta = el('textarea', { class: 'stock-obs', placeholder: 'Se falta algum produto que não está na lista, escreva aqui...' });
            ta.value = state.obsAdicional || '';
            ta.addEventListener('input', () => { state.obsAdicional = ta.value; persist(); });
            return ta; })()
        )
      ));

      // Botão de envio
      const canSend = tp.filled > 0;
      app.appendChild(el('div', { class: 'stock-submit-bar' },
        el('span', {}, canSend ? `${tp.filled} itens preenchidos` : 'Comece preenchendo alguma localização acima'),
        el('button', {
          class: 'btn btn-primary',
          disabled: !canSend ? '' : null,
          onclick: () => submitCount()
        }, resumeCountId ? 'Salvar alterações' : 'Enviar contagem')
      ));
      return;
    }

    // Tela de preenchimento de uma localização específica
    const currentLoc = locations.find(l => l.label === state.currentLocation);
    if (!currentLoc) { state.currentLocation = null; render(); return; }
    const items = itemsForLocation(template, currentLoc);
    const p = locProgress(currentLoc);

    app.appendChild(el('div', { class: 'count-loc-header' },
      el('button', { class: 'btn btn-small', onclick: () => { state.currentLocation = null; persist(); render(); } }, '← Localizações'),
      el('h2', {}, `Contando: ${currentLoc.label}`),
      el('span', { class: 'muted' }, `${p.filled} de ${p.total} itens`)
    ));

    // Barra de busca dentro da localização
    const searchInput = el('input', { type: 'search', placeholder: 'Buscar item nesta localização...', class: 'count-search-input' });
    app.appendChild(searchInput);

    // Agrupa items por seção dentro desta localização
    const sectionMap = new Map();
    items.forEach(({ section, item, colIdx }) => {
      if (!sectionMap.has(section.id)) sectionMap.set(section.id, { section, items: [] });
      sectionMap.get(section.id).items.push({ item, colIdx });
    });

    const list = el('div', { class: 'count-items-list' });
    function renderItems(filter = '') {
      list.innerHTML = '';
      const f = filter.toLowerCase().trim();
      let shown = 0;
      sectionMap.forEach(({ section, items: sItems }) => {
        const filtered = f ? sItems.filter(({ item }) => item.name.toLowerCase().includes(f)) : sItems;
        if (filtered.length === 0) return;
        const block = el('div', { class: 'count-section-block' });
        block.appendChild(el('h3', { class: 'count-sec-label' }, section.name));
        filtered.forEach(({ item, colIdx }) => {
          if (!state.values[section.id]) state.values[section.id] = {};
          if (!state.values[section.id][item.id]) state.values[section.id][item.id] = {};
          const curVal = state.values[section.id][item.id][colIdx] ?? '';
          const inp = el('input', {
            type: 'text',
            inputmode: 'decimal',
            value: curVal,
            placeholder: '0',
            class: 'count-item-input'
          });
          inp.addEventListener('input', () => {
            const cleaned = inp.value.replace(',', '.');
            state.values[section.id][item.id][colIdx] = cleaned;
            persist();
          });
          inp.addEventListener('focus', () => inp.select());
          const card = el('div', { class: 'count-item-card' + (curVal !== '' && curVal != null ? ' filled' : '') },
            el('div', { class: 'count-item-info' },
              el('div', { class: 'count-item-name' }, item.name),
              item.unit ? el('div', { class: 'count-item-unit' }, item.unit) : null
            ),
            el('div', { class: 'count-item-input-wrap' }, inp)
          );
          inp.addEventListener('input', () => {
            if (inp.value !== '' && inp.value != null) card.classList.add('filled');
            else card.classList.remove('filled');
          });
          block.appendChild(card);
          shown++;
        });
        list.appendChild(block);
      });
      if (shown === 0) list.appendChild(el('p', { class: 'muted', style: 'text-align:center;padding:2rem;' }, 'Nenhum item encontrado'));
    }
    renderItems();
    searchInput.addEventListener('input', () => renderItems(searchInput.value));
    app.appendChild(list);

    // Barra sticky: concluir localização → vai pra próxima incompleta ou pra tela de localizações
    const nextLoc = locations.find(l => {
      if (l.label === state.currentLocation) return false;
      const pr = locProgress(l);
      return pr.filled < pr.total;
    });
    app.appendChild(el('div', { class: 'stock-submit-bar' },
      el('span', { class: 'muted' }, 'Salvo automaticamente'),
      nextLoc ? el('button', { class: 'btn btn-primary', onclick: () => {
        state.currentLocation = nextLoc.label; persist(); render();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      } }, `✓ Ir pra ${nextLoc.label} →`) : el('button', { class: 'btn btn-primary', onclick: () => {
        state.currentLocation = null; persist(); render();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      } }, '✓ Concluir — ver resumo')
    ));
  }

  async function submitCount() {
    if (!confirm(resumeCountId ? 'Salvar as alterações desta contagem?' : 'Enviar esta contagem agora?')) return;
    try {
      const payload = {
        templateId: template.id,
        templateName: template.name,
        date: new Date().toISOString().slice(0, 10),
        values: state.values,
        obsAdicional: state.obsAdicional || '',
        status: 'sent',
        updatedAt: serverTimestamp()
      };
      if (!resumeCountId) {
        payload.authorUid = STATE.user.uid;
        payload.authorEmail = STATE.user.email;
        payload.authorName = STATE.userDoc?.name || STATE.user.email;
        payload.createdAt = serverTimestamp();
        const newRef = doc(stockCountsCol(cid));
        await setDoc(newRef, payload);
        await addDoc(stockCountLogsCol(cid, newRef.id), {
          timestamp: serverTimestamp(),
          user: { uid: STATE.user.uid, email: STATE.user.email, name: STATE.userDoc?.name || '' },
          action: 'submit',
          note: 'Contagem enviada'
        });
        localStorage.removeItem(lsKey);
        toast('Contagem enviada!');
        location.hash = `#/c/${cid}/estoque`;
      } else {
        const prev = STOCK_STATE.counts.find(c => c.id === resumeCountId);
        const changes = [];
        (template.sections || []).forEach(sec => {
          (sec.items || []).forEach(item => {
            sec.columns.forEach((col, idx) => {
              const before = prev?.values?.[sec.id]?.[item.id]?.[idx] ?? '';
              const after = state.values?.[sec.id]?.[item.id]?.[idx] ?? '';
              if (String(before) !== String(after)) {
                changes.push({ section: sec.name, item: item.name, column: col, before, after });
              }
            });
          });
        });
        await setDoc(stockCountDoc(cid, resumeCountId), payload, { merge: true });
        await addDoc(stockCountLogsCol(cid, resumeCountId), {
          timestamp: serverTimestamp(),
          user: { uid: STATE.user.uid, email: STATE.user.email, name: STATE.userDoc?.name || '' },
          action: 'edit',
          note: changes.length > 0 ? `${changes.length} alteração${changes.length > 1 ? 'ões' : ''}` : 'edição',
          changes
        });
        toast('Contagem atualizada!');
        location.hash = `#/c/${cid}/estoque/contagens/${resumeCountId}`;
      }
    } catch (err) { console.error(err); toast('Erro: ' + err.message); }
  }

  render();
}

function locationIcon(label) {
  const l = label.toLowerCase();
  if (l.includes('bar')) return '🍹';
  if (l.includes('adega')) return '🍷';
  if (l.includes('estoque')) return '📦';
  if (l.includes('cozinha')) return '🍳';
  if (l.includes('câmara') || l.includes('camara')) return '❄️';
  return '▣';
}

// =============== BACK: lista de contagens recebidas ===============
function renderStockCountsList(cid, app) {
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Contagens recebidas'),
      el('p', {}, 'Todas as contagens enviadas pelos templates ativos.')
    )
  ));
  // Filtros
  const filter = { templateId: 'all', dateFrom: '', dateTo: '' };
  const filterBar = el('div', { class: 'stock-filters' });
  const tplSel = el('select', {});
  tplSel.appendChild(el('option', { value: 'all' }, 'Todos os templates'));
  STOCK_STATE.templates.forEach(t => tplSel.appendChild(el('option', { value: t.id }, t.name)));
  tplSel.addEventListener('change', () => { filter.templateId = tplSel.value; renderRows(); });
  filterBar.appendChild(tplSel);
  app.appendChild(filterBar);

  const tbl = el('table', { class: 'insumos-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Data'),
      el('th', {}, 'Template'),
      el('th', {}, 'Autor'),
      el('th', {}, 'Preenchido'),
      el('th', {}, '')
    ))
  );
  const tbody = el('tbody', {});
  tbl.appendChild(tbody);

  function renderRows() {
    tbody.innerHTML = '';
    let counts = STOCK_STATE.counts;
    if (filter.templateId !== 'all') counts = counts.filter(c => c.templateId === filter.templateId);
    if (counts.length === 0) {
      tbody.appendChild(el('tr', {}, el('td', { colspan: '5', class: 'muted', style: 'text-align:center;padding:2rem;' }, 'Nenhuma contagem ainda')));
      return;
    }
    counts.forEach(c => {
      const template = STOCK_STATE.templates.find(t => t.id === c.templateId);
      const total = template ? (template.sections || []).reduce((s, sec) => s + (sec.items?.length || 0), 0) : 0;
      let filled = 0;
      (template?.sections || []).forEach(sec => {
        (sec.items || []).forEach(item => {
          const k = c.values?.[sec.id]?.[item.id];
          if (k && Object.values(k).some(v => v !== '' && v != null)) filled++;
        });
      });
      const dt = c.createdAt?.toDate ? c.createdAt.toDate() : new Date(c.date || Date.now());
      tbody.appendChild(el('tr', {},
        el('td', {}, dt.toLocaleDateString('pt-BR') + ' ' + dt.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })),
        el('td', {}, c.templateName || c.templateId),
        el('td', {}, c.authorName || c.authorEmail || '—'),
        el('td', {}, `${filled} / ${total}`),
        el('td', {},
          el('a', { class: 'btn btn-small btn-primary', href: `#/c/${cid}/estoque/contagens/${c.id}` }, 'Abrir')
        )
      ));
    });
  }
  renderRows();
  app.appendChild(el('div', { class: 'insumos-table-wrap' }, tbl));
}

// =============== BACK: detalhe/edição de contagem ===============
async function renderStockCountDetail(cid, countId, app) {
  const count = STOCK_STATE.counts.find(c => c.id === countId);
  if (!count) { app.appendChild(el('p', {}, 'Contagem não encontrada')); return; }
  const template = STOCK_STATE.templates.find(t => t.id === count.templateId);
  if (!template) { app.appendChild(el('p', {}, 'Template não encontrado')); return; }
  const canEdit = canEditContagem(cid);
  const canManage = canManageStock(cid);

  app.appendChild(el('a', { href: `#/c/${cid}/estoque/contagens`, class: 'back-link' }, '← Voltar'));
  const dt = count.createdAt?.toDate ? count.createdAt.toDate() : new Date(count.date || Date.now());
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, count.templateName),
      el('p', {}, `Enviada por ${count.authorName || count.authorEmail} em ${dt.toLocaleString('pt-BR')}`)
    ),
    el('div', { style: 'display:flex;gap:0.5rem;flex-wrap:wrap;' },
      canEdit ? el('a', { class: 'btn btn-primary', href: `#/c/${cid}/estoque/preencher/${count.templateId}/${count.id}` }, '✎ Editar') : null,
      canManage ? el('button', { class: 'btn', onclick: () => exportContagemPDF(cid, count, template) }, '↓ PDF Contagem') : null,
      canManage ? el('button', { class: 'btn btn-accent', onclick: () => exportPedidoPDF(cid, count, template) }, '↓ PDF Pedido') : null,
      canManage ? el('button', { class: 'btn', onclick: () => exportContagemXLSX(cid, count, template) }, '↓ Excel') : null,
    )
  ));

  // Logs
  try {
    const logsSnap = await getDocs(stockCountLogsCol(cid, countId));
    const logs = logsSnap.docs.map(d => ({ id: d.id, ...d.data() })).sort((a, b) => {
      const aT = a.timestamp?.toMillis?.() || 0;
      const bT = b.timestamp?.toMillis?.() || 0;
      return bT - aT;
    });
    if (logs.length > 0) {
      const logDetails = el('details', { class: 'stock-logs' },
        el('summary', {}, `Histórico de edições (${logs.length})`),
        ...logs.map(l => {
          const t = l.timestamp?.toDate ? l.timestamp.toDate() : new Date();
          return el('div', { class: 'stock-log-item' },
            el('span', { class: 'log-when' }, t.toLocaleString('pt-BR')),
            el('span', { class: 'log-who' }, l.user?.name || l.user?.email || '—'),
            el('span', { class: 'log-what' }, `${l.action}: ${l.note || ''}`),
            l.changes && l.changes.length > 0 ? el('ul', { class: 'log-changes' },
              ...l.changes.slice(0, 10).map(ch => el('li', {}, `${ch.section} / ${ch.item} — ${ch.column}: "${ch.before}" → "${ch.after}"`))
            ) : null
          );
        })
      );
      app.appendChild(logDetails);
    }
  } catch (err) { console.warn('logs load', err); }

  // Tabela de dados (por seção)
  (template.sections || []).forEach(section => {
    const sec = el('section', { class: 'stock-section' });
    sec.appendChild(el('header', { class: 'stock-sec-head' },
      el('h2', {}, section.name),
      section.team ? el('span', { class: 'muted' }, section.team) : null
    ));
    const tbl = el('table', { class: 'insumos-table' },
      el('thead', {}, el('tr', {},
        el('th', {}, 'Item'),
        ...section.columns.map(c => el('th', {}, c))
      ))
    );
    const tbody = el('tbody', {});
    (section.items || []).forEach(item => {
      const row = el('tr', {});
      row.appendChild(el('td', {}, item.name, item.unit ? el('span', { class: 'muted unit-tag' }, item.unit) : null));
      section.columns.forEach((col, idx) => {
        const val = count.values?.[section.id]?.[item.id]?.[idx] ?? '';
        row.appendChild(el('td', { class: 'num' }, val === '' ? '—' : val));
      });
      tbody.appendChild(row);
    });
    tbl.appendChild(tbody);
    sec.appendChild(tbl);
    app.appendChild(sec);
  });
  if (count.obsAdicional) {
    app.appendChild(el('div', { class: 'info-box' },
      el('strong', {}, 'Observação adicional:'),
      el('p', {}, count.obsAdicional)
    ));
  }
}

// =============== EXPORTS ===============
function exportContagemPDF(cid, count, template) {
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const margin = 15;
  let y = margin;
  const pageWidth = docPdf.internal.pageSize.getWidth();
  const cliente = STATE.currentCliente;
  docPdf.setFont('times', 'italic').setFontSize(10).setTextColor(130);
  docPdf.text(cliente?.name || '', pageWidth / 2, y, { align: 'center' }); y += 5;
  docPdf.setFont('times', 'normal').setFontSize(18).setTextColor(20);
  docPdf.text(count.templateName || 'Contagem', pageWidth / 2, y, { align: 'center' }); y += 7;
  const dt = count.createdAt?.toDate ? count.createdAt.toDate() : new Date();
  docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(100);
  docPdf.text(`${dt.toLocaleDateString('pt-BR')} — ${count.authorName || count.authorEmail}`, pageWidth / 2, y, { align: 'center' }); y += 10;

  (template.sections || []).forEach(section => {
    if (y > 250) { docPdf.addPage(); y = margin; }
    docPdf.setFont('times', 'bold').setFontSize(12).setTextColor(30);
    docPdf.text(section.name, margin, y); y += 5;
    docPdf.autoTable({
      startY: y, margin: { left: margin, right: margin },
      head: [['Item', ...section.columns]],
      body: (section.items || []).map(item => [
        item.name + (item.unit ? ` (${item.unit})` : ''),
        ...section.columns.map((col, idx) => count.values?.[section.id]?.[item.id]?.[idx] ?? '—')
      ]),
      theme: 'plain', styles: { fontSize: 9 },
      headStyles: { fillColor: [247, 245, 238], fontStyle: 'bold', fontSize: 8 }
    });
    y = docPdf.lastAutoTable.finalY + 6;
  });
  if (count.obsAdicional) {
    if (y > 250) { docPdf.addPage(); y = margin; }
    docPdf.setFont('times', 'bold').setFontSize(11); docPdf.text('Observações:', margin, y); y += 5;
    docPdf.setFont('helvetica', 'normal').setFontSize(10); docPdf.text(docPdf.splitTextToSize(count.obsAdicional, pageWidth - margin * 2), margin, y);
  }
  docPdf.save(`contagem-${dt.toISOString().slice(0, 10)}.pdf`);
  toast('PDF gerado');
}

function exportPedidoPDF(cid, count, template) {
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const margin = 15;
  let y = margin;
  const pageWidth = docPdf.internal.pageSize.getWidth();
  const cliente = STATE.currentCliente;
  docPdf.setFont('times', 'italic').setFontSize(10).setTextColor(130);
  docPdf.text(cliente?.name || '', pageWidth / 2, y, { align: 'center' }); y += 5;
  docPdf.setFont('times', 'normal').setFontSize(18).setTextColor(20);
  docPdf.text('Pedido de Compra', pageWidth / 2, y, { align: 'center' }); y += 7;
  const dt = count.createdAt?.toDate ? count.createdAt.toDate() : new Date();
  docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(100);
  docPdf.text(`Baseado na contagem de ${dt.toLocaleDateString('pt-BR')}`, pageWidth / 2, y, { align: 'center' }); y += 10;

  // Um pedido por fornecedor (seção), considerando a coluna "Pedido" se existir
  (template.sections || []).forEach(section => {
    const pedidoIdx = section.columns.findIndex(c => c.toLowerCase().includes('pedido'));
    if (pedidoIdx < 0) return; // seção sem coluna de pedido
    const rows = (section.items || []).map(item => {
      const qty = count.values?.[section.id]?.[item.id]?.[pedidoIdx] ?? '';
      if (qty === '' || qty == null || parseFloat(qty) === 0) return null;
      return [item.name, qty, item.unit || ''];
    }).filter(Boolean);
    if (rows.length === 0) return;
    if (y > 240) { docPdf.addPage(); y = margin; }
    docPdf.setFont('times', 'bold').setFontSize(12).setTextColor(30);
    docPdf.text(section.name, margin, y); y += 5;
    docPdf.autoTable({
      startY: y, margin: { left: margin, right: margin },
      head: [['Item', 'Qty', 'Unidade']],
      body: rows,
      theme: 'grid', styles: { fontSize: 9 },
      headStyles: { fillColor: [60, 60, 60], textColor: [255, 255, 255], fontStyle: 'bold' },
      columnStyles: { 1: { halign: 'right' } }
    });
    y = docPdf.lastAutoTable.finalY + 8;
  });

  const finalY = Math.max(y, 250);
  docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(80);
  docPdf.text('Aprovado por: ____________________', margin, finalY);
  docPdf.text('Data/hora: ____________________', pageWidth - margin - 70, finalY);
  docPdf.save(`pedido-${dt.toISOString().slice(0, 10)}.pdf`);
  toast('PDF gerado');
}

function exportContagemXLSX(cid, count, template) {
  const XLSX = window.XLSX;
  const wb = XLSX.utils.book_new();
  // Resumo
  const resumo = [[count.templateName], [`Autor: ${count.authorName || count.authorEmail}`], [`Data: ${count.date || ''}`], []];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(resumo), 'Resumo');
  // Uma aba por seção (ou por grupo, simplificado: uma aba 'Contagem completa' com tudo)
  const contagemData = [['Seção', 'Item', 'Unidade', ...(new Set((template.sections || []).flatMap(s => s.columns)))]];
  // Abordagem mais simples: uma coluna por coluna de seção
  const allColsSet = new Set();
  (template.sections || []).forEach(s => s.columns.forEach(c => allColsSet.add(c)));
  const allCols = [...allColsSet];
  const data = [['Seção', 'Item', 'Unidade', ...allCols]];
  (template.sections || []).forEach(section => {
    (section.items || []).forEach(item => {
      const rowVals = allCols.map(col => {
        const idx = section.columns.indexOf(col);
        if (idx < 0) return '';
        return count.values?.[section.id]?.[item.id]?.[idx] ?? '';
      });
      data.push([section.name, item.name, item.unit || '', ...rowVals]);
    });
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(data), 'Contagem');
  // Pedido
  const pedidoData = [['Pedido de Compra'], [`Data: ${count.date || ''}`], [], ['Fornecedor', 'Item', 'Qty', 'Unidade']];
  (template.sections || []).forEach(section => {
    const pedidoIdx = section.columns.findIndex(c => c.toLowerCase().includes('pedido'));
    if (pedidoIdx < 0) return;
    (section.items || []).forEach(item => {
      const qty = count.values?.[section.id]?.[item.id]?.[pedidoIdx] ?? '';
      if (qty === '' || qty == null || parseFloat(qty) === 0) return;
      pedidoData.push([section.name, item.name, qty, item.unit || '']);
    });
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(pedidoData), 'Pedido de Compra');
  if (count.obsAdicional) {
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet([['Observação adicional'], [count.obsAdicional]]), 'Observação');
  }
  const dt = count.createdAt?.toDate ? count.createdAt.toDate() : new Date();
  XLSX.writeFile(wb, `contagem-${dt.toISOString().slice(0, 10)}.xlsx`);
  toast('Excel gerado');
}

// =============== TEMPLATES: lista admin ===============
function renderStockTemplatesAdminList(cid, app) {
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Templates de contagem'),
      el('p', {}, 'Crie e gerencie templates de contagem.')
    ),
    el('div', { style: 'display:flex;gap:0.5rem;' },
      el('button', { class: 'btn', onclick: async () => {
        if (!confirm('Criar o template inicial do Bar Sororoca?')) return;
        try { await seedSororocaTemplate(cid); renderEstoque(cid, ['templates']); } catch (e) { toast('Erro: ' + e.message); }
      } }, '↻ Seed Sororoca'),
      el('a', { class: 'btn btn-primary', href: `#/c/${cid}/estoque/templates/novo` }, '+ Novo template')
    )
  ));
  if (STOCK_STATE.templates.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' }, el('p', {}, 'Nenhum template cadastrado.')));
    return;
  }
  const list = el('div', { class: 'dish-admin-list' });
  STOCK_STATE.templates.forEach(t => {
    const itemCount = (t.sections || []).reduce((s, sec) => s + (sec.items?.length || 0), 0);
    list.appendChild(el('div', { class: 'dish-admin-item' },
      el('div', { class: 'info' },
        el('h4', {}, t.name, t.active === false ? el('span', { class: 'muted' }, ' (inativo)') : null),
        el('p', {}, `${(t.sections || []).length} seções · ${itemCount} itens`)
      ),
      el('div', { class: 'dish-admin-actions' },
        el('a', { class: 'btn btn-small btn-primary', href: `#/c/${cid}/estoque/templates/editar/${t.id}` }, 'Editar'),
        el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
          if (!confirm(`Excluir "${t.name}"?`)) return;
          try { await deleteDoc(stockTemplateDoc(cid, t.id)); toast('Excluído'); renderEstoque(cid, ['templates']); } catch (e) { toast('Erro: ' + e.message); }
        } }, 'Excluir')
      )
    ));
  });
  app.appendChild(list);
}

// =============== TEMPLATES: editor ===============
function renderStockTemplateEdit(cid, templateId, app) {
  let template;
  if (templateId) {
    template = JSON.parse(JSON.stringify(STOCK_STATE.templates.find(t => t.id === templateId) || {}));
    if (!template.id) { app.appendChild(el('p', {}, 'Template não encontrado')); return; }
  } else {
    template = { type: 'stock_count', name: '', active: true, sections: [] };
  }
  app.appendChild(el('a', { href: `#/c/${cid}/estoque/templates`, class: 'back-link' }, '← Voltar'));
  app.appendChild(el('h1', {}, templateId ? 'Editar template' : 'Novo template'));
  const panel = el('div', { class: 'admin-panel' });
  panel.appendChild(el('div', { class: 'form-grid' },
    (() => { const i = el('input', { type: 'text', value: template.name || '', placeholder: 'Ex: Contagem semanal do bar' });
      i.addEventListener('input', () => template.name = i.value);
      return el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome do template'), i); })(),
    (() => { const i = el('input', { type: 'checkbox' });
      if (template.active !== false) i.setAttribute('checked', '');
      i.addEventListener('change', () => template.active = i.checked);
      return el('label', { class: 'field field-inline' }, i, el('span', { class: 'label-text', style: 'margin-left:0.5rem;' }, 'Ativo (aparece pra equipe preencher)')); })()
  ));

  panel.appendChild(el('h2', {}, 'Seções'));
  const secWrap = el('div', {});
  function renderSecs() {
    secWrap.innerHTML = '';
    (template.sections || []).forEach((section, sIdx) => {
      if (!section.id) section.id = 'sec-' + uid();
      if (!section.columns) section.columns = ['Pedido', 'Contagem', 'Estoque'];
      if (!section.items) section.items = [];
      const box = el('details', { class: 'subficha-editor', open: '' });
      box.appendChild(el('summary', { class: 'subficha-editor-summary' },
        el('span', { class: 'sf-editor-num' }, String(sIdx + 1).padStart(2, '0')),
        el('span', { class: 'sf-editor-name' }, section.name || '(nova seção)'),
        el('span', { class: 'sf-editor-meta' }, `${(section.items || []).length} itens`),
        el('span', { class: 'sf-editor-chev' }, '▾'),
        el('span', { class: 'sf-editor-actions', onclick: e => e.stopPropagation() },
          el('button', { class: 'btn btn-small btn-danger', onclick: (e) => { e.preventDefault(); if (confirm('Excluir seção?')) { template.sections.splice(sIdx, 1); renderSecs(); } } }, '×')
        )
      ));
      const sForm = el('div', { class: 'form-grid' },
        (() => { const i = el('input', { type: 'text', value: section.name || '', placeholder: 'Ex: Vinhos Europa' });
          i.addEventListener('input', () => section.name = i.value);
          return el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome da seção (fornecedor)'), i); })(),
        (() => { const i = el('input', { type: 'text', value: section.group || '', placeholder: 'Ex: Pedidos Vinhos' });
          i.addEventListener('input', () => section.group = i.value);
          return el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Grupo (opcional)'), i); })(),
        (() => { const i = el('input', { type: 'text', value: section.team || '', placeholder: 'Ex: Equipe do Salão' });
          i.addEventListener('input', () => section.team = i.value);
          return el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Time (opcional)'), i); })(),
        (() => { const i = el('input', { type: 'text', value: section.columns.join(', '), placeholder: 'Pedido, Contagem Bar, Contagem Estoque' });
          i.addEventListener('input', () => section.columns = i.value.split(',').map(s => s.trim()).filter(Boolean));
          return el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Colunas (separadas por vírgula)'), i); })()
      );
      box.appendChild(sForm);
      box.appendChild(el('h4', {}, 'Itens'));
      const itemsList = el('div', {});
      function renderItems() {
        itemsList.innerHTML = '';
        section.items.forEach((item, iIdx) => {
          if (!item.id) item.id = 'it-' + uid();
          const row = el('div', { class: 'ingredient-row', style: 'grid-template-columns: 3fr 1fr auto;' });
          const n = el('input', { type: 'text', value: item.name || '', placeholder: 'Nome do item' });
          n.addEventListener('input', () => item.name = n.value);
          row.appendChild(n);
          const u = el('input', { type: 'text', value: item.unit || '', placeholder: 'Unidade' });
          u.addEventListener('input', () => item.unit = u.value);
          row.appendChild(u);
          row.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: () => { section.items.splice(iIdx, 1); renderItems(); } }, '×'));
          itemsList.appendChild(row);
        });
        itemsList.appendChild(el('button', { class: 'btn btn-small', onclick: () => {
          section.items.push({ id: 'it-' + uid(), name: '', unit: '' });
          renderItems();
        } }, '+ Adicionar item'));
      }
      renderItems();
      box.appendChild(itemsList);
      secWrap.appendChild(box);
    });
    secWrap.appendChild(el('button', { class: 'btn', onclick: () => {
      template.sections.push({ id: 'sec-' + uid(), name: 'Nova seção', group: '', team: '', columns: ['Pedido', 'Contagem', 'Estoque'], items: [] });
      renderSecs();
    } }, '+ Adicionar seção'));
  }
  renderSecs();
  panel.appendChild(secWrap);

  panel.appendChild(el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn btn-primary', onclick: async () => {
      if (!template.name.trim()) { alert('Nome obrigatório'); return; }
      try {
        const id = templateId || ('tpl-' + uid());
        template.id = id;
        const payload = { ...template, updatedAt: serverTimestamp() };
        if (!templateId) payload.createdAt = serverTimestamp();
        delete payload.id;
        await setDoc(stockTemplateDoc(cid, id), payload, { merge: true });
        toast('Template salvo');
        location.hash = `#/c/${cid}/estoque/templates`;
      } catch (err) { toast('Erro: ' + err.message); }
    } }, 'Salvar template'),
    el('a', { class: 'btn', href: `#/c/${cid}/estoque/templates` }, 'Cancelar')
  ));
  app.appendChild(panel);
}

function renderProducao(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));

  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Plano de produção'),
      el('p', {}, `Selecione os pratos e quantidades para o dia. Lista de compras e requisição de estoque são geradas automaticamente.`)
    ),
    el('div', {},
      el('button', { class: 'btn btn-primary', onclick: () => openAddDishToPlanModal(cid) }, '+ Adicionar prato')
    )
  ));

  const container = el('div', { class: 'producao-wrap' });
  const planList = el('div', { class: 'prod-plan-list' });
  const summary = el('div', { class: 'prod-summary' });
  const subfichasOut = el('div', { class: 'prod-subfichas' });
  const shoppingOut = el('div', { class: 'prod-shopping' });

  function recompute() {
    planList.innerHTML = '';
    summary.innerHTML = '';
    subfichasOut.innerHTML = '';
    shoppingOut.innerHTML = '';

    if (PROD_PLAN.items.length === 0) {
      planList.appendChild(el('div', { class: 'empty-state' },
        el('p', {}, 'Nenhum prato no plano.'),
        el('button', { class: 'btn btn-primary', onclick: () => openAddDishToPlanModal(cid) }, '+ Adicionar prato')
      ));
      return;
    }

    let totalCost = 0;
    let totalPortions = 0;

    PROD_PLAN.items.forEach((item, itemIdx) => {
      const dish = STATE.dishes.find(d => d.id === item.dishId);
      if (!dish) return;
      const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
      const origRend = getSfRendimento(finalSf);
      // Escala: qty alvo / rendimento original (se unidades batem)
      let scale = 1;
      if (item.targetQty > 0 && origRend.qty > 0) {
        const [qConv] = normalizeSubrefQty(item.targetQty, item.targetUnit, origRend.unit);
        scale = qConv / origRend.qty;
      }
      const scales = computeCascadeScales(dish, scale * origRend.qty);
      const cost = dishCost(dish);
      const itemTotalCost = cost.total * scale;
      totalCost += itemTotalCost;

      const itemCard = el('article', { class: 'prod-item' });
      const head = el('div', { class: 'prod-item-head' },
        el('div', { class: 'prod-item-name' },
          el('span', { class: 'prod-item-num' }, String(itemIdx + 1).padStart(2, '0')),
          el('span', {}, dish.name)
        ),
        el('div', { class: 'prod-item-controls' },
          (() => {
            // Garante que a unidade-alvo é sempre a unidade da ficha final cadastrada
            item.targetUnit = origRend.unit || item.targetUnit || '';
            const qtyInput = el('input', { type: 'text', inputmode: 'numeric', value: item.targetQty || '', class: 'prod-qty-input', placeholder: '0' });
            // Atualiza estado em cada keystroke, mas só faz o recompute pesado no blur/change
            // (recompute destrói o input e perderia o foco)
            qtyInput.addEventListener('input', () => {
              // Aceita só dígitos e ponto (sem vírgula)
              const cleaned = qtyInput.value.replace(/[^0-9.]/g, '');
              if (cleaned !== qtyInput.value) qtyInput.value = cleaned;
              const v = parseFloat(cleaned);
              item.targetQty = isNaN(v) ? 0 : v;
            });
            qtyInput.addEventListener('change', () => recompute());
            qtyInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); qtyInput.blur(); } });
            return qtyInput;
          })(),
          el('span', { class: 'prod-unit-fixed' }, origRend.unit || ''),
          el('span', { class: 'prod-scale muted' }, scale > 0 ? `${fmtNum(scale, 2)}×` : '—'),
          el('span', { class: 'prod-item-cost' }, fmtBRL(itemTotalCost)),
          el('button', { class: 'btn btn-small btn-danger', onclick: () => {
            PROD_PLAN.items.splice(itemIdx, 1); recompute();
          } }, '×')
        )
      );
      itemCard.appendChild(head);

      // Sub-fichas checkboxes
      const sfBox = el('div', { class: 'prod-sf-box' });
      if (!item.excludedSfIds) item.excludedSfIds = new Set();
      dish.sub_fichas.forEach((sf, sfIdx) => {
        const isFinal = sfIdx === dish.sub_fichas.length - 1;
        const sfScale = scales[sf.id] || 1;
        const sfR = getSfRendimento(sf);
        const scaledQty = sfR.qty * sfScale;
        const sn = normUnitForDisplay(scaledQty, sfR.unit);
        const checked = !item.excludedSfIds.has(sf.id);
        const chk = el('input', { type: 'checkbox' });
        if (checked) chk.setAttribute('checked', '');
        chk.addEventListener('change', () => {
          if (chk.checked) item.excludedSfIds.delete(sf.id);
          else item.excludedSfIds.add(sf.id);
          recompute();
        });
        sfBox.appendChild(el('label', { class: 'prod-sf-check' }, chk,
          el('span', { class: 'prod-sf-name' + (isFinal ? ' is-final' : '') }, sf.name),
          el('span', { class: 'prod-sf-rend muted' }, `${sn.text} ${sn.unit}`.trim())
        ));
      });
      itemCard.appendChild(sfBox);
      planList.appendChild(itemCard);

      // Conta porções se unidade é 'porções' ou 'porção' ou 'und'
      const unitLower = (item.targetUnit || origRend.unit || '').toLowerCase();
      if (['porção', 'porçao', 'porções', 'porcoes', 'und'].includes(unitLower)) {
        totalPortions += item.targetQty;
      }
    });

    // Resumo
    summary.appendChild(el('div', { class: 'prod-summary-card' },
      el('div', { class: 'prod-sum-row' },
        el('span', { class: 'muted' }, 'Custo total estimado'),
        el('strong', {}, fmtBRL(totalCost))
      ),
      totalPortions > 0 ? el('div', { class: 'prod-sum-row' },
        el('span', { class: 'muted' }, 'Porções'),
        el('strong', {}, fmtNum(totalPortions, 0))
      ) : null,
      el('div', { class: 'prod-actions' },
        el('button', { class: 'btn btn-small', onclick: () => exportProducaoPDF(cid) }, '↓ PDF Produção'),
        el('button', { class: 'btn btn-small', onclick: () => openRequisicaoOptionsModal(cid) }, '↓ PDF Requisição'),
        el('button', { class: 'btn btn-small', onclick: () => exportProducaoXLSX(cid) }, '↓ Excel')
      )
    ));

    // Sub-fichas a produzir (agrupadas por prato)
    subfichasOut.appendChild(el('h2', { class: 'prod-section-title' }, 'Sub-fichas a produzir'));
    PROD_PLAN.items.forEach(item => {
      const dish = STATE.dishes.find(d => d.id === item.dishId);
      if (!dish) return;
      const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
      const origRend = getSfRendimento(finalSf);
      let scale = 1;
      if (item.targetQty > 0 && origRend.qty > 0) {
        const [qConv] = normalizeSubrefQty(item.targetQty, item.targetUnit, origRend.unit);
        scale = qConv / origRend.qty;
      }
      const scales = computeCascadeScales(dish, scale * origRend.qty);
      const activeSfs = dish.sub_fichas.filter(sf => !item.excludedSfIds.has(sf.id));
      if (activeSfs.length === 0) {
        subfichasOut.appendChild(el('div', { class: 'prod-dish-block' },
          el('h3', { class: 'prod-dish-name' }, dish.name),
          el('p', { class: 'muted' }, 'Nenhuma sub-ficha selecionada — só usa estoque.')
        ));
        return;
      }
      const block = el('div', { class: 'prod-dish-block' },
        el('h3', { class: 'prod-dish-name' }, `${dish.name} — ${fmtNum(item.targetQty, 2)} ${item.targetUnit || origRend.unit}`)
      );
      const list = el('div', { class: 'prod-sf-produce-list' });
      activeSfs.forEach(sf => {
        const sfScale = scales[sf.id] || 1;
        const sfR = getSfRendimento(sf);
        const sn = normUnitForDisplay(sfR.qty * sfScale, sfR.unit);
        list.appendChild(el('div', { class: 'prod-sf-produce-item' },
          el('span', { class: 'prod-sf-bullet' }, '•'),
          el('a', { href: `#/c/${cid}/ficha/${dish.id}/${sf.id}` }, sf.name),
          el('span', { class: 'muted' }, ` — ${sn.text} ${sn.unit}`.trimEnd())
        ));
      });
      block.appendChild(list);
      subfichasOut.appendChild(block);
    });

    // Lista de compras agregada
    shoppingOut.appendChild(el('h2', { class: 'prod-section-title' }, 'Lista de compras consolidada'));
    const shoppingAgg = buildProducaoShoppingList();
    if (shoppingAgg.length === 0) {
      shoppingOut.appendChild(el('p', { class: 'muted' }, 'Nenhum insumo a comprar (todas sub-fichas desmarcadas).'));
    } else {
      const tbl = el('table', { class: 'prod-shopping-table' },
        el('thead', {}, el('tr', {},
          el('th', {}, 'Insumo'),
          el('th', { class: 'num' }, 'Qtd total'),
          el('th', {}, 'Unidade'),
          el('th', {}, 'De onde vem')
        )),
        el('tbody', {}, ...shoppingAgg.map(row => {
          const sources = row.sources.map(s => `${s.dishName} / ${s.sfName} (${fmtNum(s.qty, 3)} ${s.unit})`).join('\n');
          const norm = normUnitForDisplay(row.qty, row.unit);
          return el('tr', {},
            el('td', {}, row.name, row.isReutilizavel ? el('span', { class: 'reut-badge-inline' }, 'rateio') : null),
            el('td', { class: 'num' }, norm.text),
            el('td', {}, norm.unit),
            el('td', { class: 'prod-sources-cell', title: sources }, `${row.sources.length} sub-ficha${row.sources.length > 1 ? 's' : ''}`)
          );
        }))
      );
      shoppingOut.appendChild(tbl);
    }
  }

  function buildProducaoShoppingList() {
    const agg = {}; // key: insumo_id
    PROD_PLAN.items.forEach(item => {
      const dish = STATE.dishes.find(d => d.id === item.dishId);
      if (!dish) return;
      const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
      const origRend = getSfRendimento(finalSf);
      let scale = 1;
      if (item.targetQty > 0 && origRend.qty > 0) {
        const [qConv] = normalizeSubrefQty(item.targetQty, item.targetUnit, origRend.unit);
        scale = qConv / origRend.qty;
      }
      const scales = computeCascadeScales(dish, scale * origRend.qty);
      for (const sf of dish.sub_fichas) {
        if (item.excludedSfIds.has(sf.id)) continue;
        const sfScale = scales[sf.id] || 1;
        for (const ing of sf.ingredientes || []) {
          if (ing.subref_id) continue;
          if (ing.qty == null) continue;
          // Pula subprodutos (não compra)
          const subprodHit = findSubproduto(ing.insumo_name);
          if (subprodHit && !(subprodHit.dish.id === dish.id && subprodHit.sf.id === sf.id)) continue;
          const insumo = findInsumo(ing.insumo_id);
          if (!insumo) continue;
          const [qn, pn] = normalizeQtyPrice(ing, insumo);
          if (qn == null || pn == null) continue;
          const isReut = !!insumo.reutilizavel;
          const fc = isReut ? 1 : (ing.fc || 1);
          const key = ing.insumo_id;
          if (!agg[key]) agg[key] = { insumo_id: ing.insumo_id, name: insumo.name, unit: insumo.unit, qty: 0, cost: 0, isReutilizavel: isReut, sources: [] };
          const addedQty = qn * sfScale * fc;
          agg[key].qty += addedQty;
          agg[key].cost += isReut ? 0 : (qn * pn * sfScale * fc);
          agg[key].sources.push({ dishName: dish.name, sfName: sf.name, qty: addedQty, unit: insumo.unit });
        }
      }
    });
    return Object.values(agg).sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
  }
  window.__prodBuildShopping = buildProducaoShoppingList; // expose for export

  container.appendChild(summary);
  container.appendChild(planList);
  container.appendChild(subfichasOut);
  container.appendChild(shoppingOut);
  app.appendChild(container);
  recompute();
}

function openAddDishToPlanModal(cid) {
  const modal = el('div', { class: 'modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() })
  );
  const content = el('div', { class: 'modal-content modal-wide' },
    el('h2', {}, 'Adicionar prato ao plano'),
    el('p', { class: 'modal-subtitle' }, 'Selecione um prato. Você pode definir a quantidade depois.')
  );
  const searchInput = el('input', { type: 'search', placeholder: 'Buscar prato...', class: 'copy-sf-search' });
  content.appendChild(searchInput);
  const list = el('div', { class: 'copy-sf-list' });
  function renderList() {
    list.innerHTML = '';
    const q = searchInput.value.toLowerCase().trim();
    const inPlan = new Set(PROD_PLAN.items.map(i => i.dishId));
    STATE.dishes.forEach(dish => {
      if (q && !dish.name.toLowerCase().includes(q)) return;
      if (inPlan.has(dish.id)) return;
      const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
      const rend = finalSf?.rendimento || '—';
      const btn = el('button', { class: 'copy-sf-item', onclick: () => {
        const finalR = getSfRendimento(finalSf);
        PROD_PLAN.items.push({
          dishId: dish.id,
          targetQty: finalR.qty || 1,
          targetUnit: finalR.unit || '',
          excludedSfIds: new Set()
        });
        modal.remove();
        renderProducao(cid);
      } },
        el('div', { class: 'copy-sf-item-name' }, dish.name),
        el('div', { class: 'copy-sf-item-meta' }, `${(dish.sub_fichas || []).length} sub-fichas · rend. ${rend}`)
      );
      list.appendChild(btn);
    });
    if (!list.children.length) list.appendChild(el('p', { class: 'muted' }, inPlan.size >= STATE.dishes.length ? 'Todos os pratos já estão no plano.' : 'Nenhum prato encontrado.'));
  }
  renderList();
  searchInput.addEventListener('input', renderList);
  content.appendChild(list);
  content.appendChild(el('div', { class: 'modal-actions' },
    el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar')
  ));
  modal.appendChild(content);
  document.body.appendChild(modal);
  searchInput.focus();
}

// ─── Layout helpers compartilhados pelos PDFs ───
// Paleta alinhada com o app web (planejamento.app style): índigo accent + cinzas neutros
const PDF_COLORS = {
  ink:         [17, 24, 39],    // gray-900 (--text)
  body:        [55, 65, 81],    // gray-700 (--text2)
  muted:       [107, 114, 128], // gray-500 (--text3)
  subtle:      [156, 163, 175], // gray-400 (--text4)
  hairline:    [229, 231, 235], // gray-200 (--border)
  zebra:       [249, 250, 251], // gray-50 (--bg2)
  accent:      [99, 102, 241],  // indigo-500 (--accent)
  accentDark:  [67, 56, 202],   // indigo-700 (--accent-text)
  accentLight: [238, 242, 255]  // indigo-50 (--accent-bg)
};
const PDF_LAYOUT = {
  margin: 16,
  pageHeight: 297,
  pageWidth: 210
};

function pdfDrawHeader(docPdf, title, subtitle) {
  const M = PDF_LAYOUT.margin;
  const W = PDF_LAYOUT.pageWidth;
  const cliente = STATE.currentCliente;
  let y = M;
  // Marca da consultoria + restaurante (linha única no topo, pequeno)
  docPdf.setFont('helvetica', 'bold').setFontSize(8).setTextColor(...PDF_COLORS.accent);
  docPdf.text('APPMISE', M, y);
  if (cliente?.name) {
    docPdf.setFont('helvetica', 'normal').setFontSize(8).setTextColor(...PDF_COLORS.muted);
    docPdf.text(' · ' + cliente.name, M + 18, y);
  }
  // Data à direita
  docPdf.setFont('helvetica', 'normal').setFontSize(8).setTextColor(...PDF_COLORS.muted);
  docPdf.text(new Date().toLocaleDateString('pt-BR', { day: '2-digit', month: '2-digit', year: 'numeric' }), W - M, y, { align: 'right' });
  y += 6;
  // Linha índigo decorativa fina sob o cabeçalho
  docPdf.setDrawColor(...PDF_COLORS.accent).setLineWidth(0.6);
  docPdf.line(M, y, M + 20, y); y += 6;
  // Título principal — DM Sans peso bold (jsPDF: helvetica bold)
  docPdf.setFont('helvetica', 'bold').setFontSize(20).setTextColor(...PDF_COLORS.ink);
  docPdf.text(title, M, y); y += 4;
  // Subtítulo
  if (subtitle) {
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.muted);
    docPdf.text(subtitle, M, y); y += 5;
  } else { y += 2; }
  // Linha hairline embaixo
  docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
  docPdf.line(M, y, W - M, y); y += 7;
  return y;
}

function pdfDrawFooter(docPdf) {
  const M = PDF_LAYOUT.margin;
  const W = PDF_LAYOUT.pageWidth;
  const H = PDF_LAYOUT.pageHeight;
  const total = docPdf.internal.getNumberOfPages();
  for (let i = 1; i <= total; i++) {
    docPdf.setPage(i);
    docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
    docPdf.line(M, H - 12, W - M, H - 12);
    docPdf.setFont('helvetica', 'normal').setFontSize(7.5).setTextColor(...PDF_COLORS.subtle);
    docPdf.text('appmise.app', M, H - 7);
    docPdf.text(`${i} de ${total}`, W - M, H - 7, { align: 'right' });
  }
}

// Helper: monta resumo de pratos × quantidades pro topo dos PDFs
function buildProdResumoLines() {
  return PROD_PLAN.items
    .filter(it => it.targetQty > 0)
    .map(it => {
      const d = STATE.dishes.find(x => x.id === it.dishId);
      if (!d) return null;
      return { name: d.name, qty: it.targetQty, unit: it.targetUnit };
    })
    .filter(Boolean);
}

// PDF de Produção: roteiro detalhado pra equipe de cozinha
function exportProducaoPDF(cid) {
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const M = PDF_LAYOUT.margin;
  const W = PDF_LAYOUT.pageWidth;
  const H = PDF_LAYOUT.pageHeight;
  let y;

  function ensureSpace(needed) {
    if (y + needed > H - 16) { docPdf.addPage(); y = M; }
  }

  // ── Cabeçalho ──
  y = pdfDrawHeader(docPdf, 'Roteiro de Produção', 'Para a equipe de cozinha');

  // ── Resumo do plano ──
  const resumo = buildProdResumoLines();
  if (resumo.length === 0) {
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.body);
    docPdf.text('Nenhum prato no plano. Defina quantidades antes de gerar o PDF.', M, y);
    pdfDrawFooter(docPdf);
    docPdf.save(`roteiro-producao-${new Date().toISOString().slice(0, 10)}.pdf`);
    return;
  }
  // Card de resumo — border + bg cinza claro (estilo "card" do app)
  const resumoH = 9 + resumo.length * 5.5;
  docPdf.setFillColor(...PDF_COLORS.zebra);
  docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
  docPdf.roundedRect(M, y, W - 2 * M, resumoH, 1.5, 1.5, 'FD');
  docPdf.setFont('helvetica', 'bold').setFontSize(7.5).setTextColor(...PDF_COLORS.accentDark);
  docPdf.text('PRATOS PLANEJADOS', M + 4, y + 5);
  docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.ink);
  resumo.forEach((r, i) => {
    docPdf.text(r.name, M + 4, y + 10.5 + i * 5.5);
    docPdf.setFont('helvetica', 'bold').setTextColor(...PDF_COLORS.body);
    docPdf.text(`${fmtNum(r.qty, 0)} ${r.unit}`, W - M - 4, y + 10.5 + i * 5.5, { align: 'right' });
    docPdf.setFont('helvetica', 'normal').setTextColor(...PDF_COLORS.ink);
  });
  y += resumoH + 8;

  // ── Roteiro por prato ──
  PROD_PLAN.items.forEach((item, itemIdx) => {
    if (!(item.targetQty > 0)) return;
    const dish = STATE.dishes.find(d => d.id === item.dishId);
    if (!dish) return;
    const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
    const origRend = getSfRendimento(finalSf);
    let scale = 1;
    if (origRend.qty > 0) {
      const [qConv] = normalizeSubrefQty(item.targetQty, item.targetUnit, origRend.unit);
      scale = qConv / origRend.qty;
    }
    const scales = computeCascadeScales(dish, scale * origRend.qty);
    const activeSfs = dish.sub_fichas.filter(sf => !item.excludedSfIds.has(sf.id));
    if (activeSfs.length === 0) return;

    // Page break entre pratos (mas não antes do primeiro)
    if (itemIdx > 0) { docPdf.addPage(); y = M; }

    // Cabeçalho do prato — texto grande limpo (sem bloco preto)
    ensureSpace(36);
    // Eyebrow pequeno
    docPdf.setFont('helvetica', 'normal').setFontSize(7.5).setTextColor(...PDF_COLORS.muted);
    docPdf.text(`PRATO ${String(itemIdx + 1).padStart(2, '0')} DE ${PROD_PLAN.items.length}`, M, y);
    y += 7;
    // Título grande
    docPdf.setFont('helvetica', 'bold').setFontSize(18).setTextColor(...PDF_COLORS.ink);
    docPdf.text(dish.name, M, y);
    y += 8;
    // Sublinha "Produzir X porções"
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.body);
    docPdf.text(`Produzir ${fmtNum(item.targetQty, 0)} ${item.targetUnit}`, M, y);
    y += 4;
    // Linha índigo decorativa
    docPdf.setDrawColor(...PDF_COLORS.accent).setLineWidth(0.6);
    docPdf.line(M, y, M + 20, y);
    y += 9;

    // Cada sub-ficha na ordem
    activeSfs.forEach((sf, sfIdx) => {
      const sfScale = scales[sf.id] || 1;
      const sfR = getSfRendimento(sf);
      const sn = normUnitForDisplay(sfR.qty * sfScale, sfR.unit);
      const isFinal = dish.sub_fichas.indexOf(sf) === dish.sub_fichas.length - 1;

      ensureSpace(28);
      // Número em chip arredondado + nome + label final
      const chipSize = 8;
      if (isFinal) {
        docPdf.setFillColor(...PDF_COLORS.accent);
      } else {
        docPdf.setFillColor(...PDF_COLORS.accentLight);
      }
      docPdf.roundedRect(M, y, chipSize, chipSize, 1.5, 1.5, 'F');
      docPdf.setFont('helvetica', 'bold').setFontSize(7.5);
      docPdf.setTextColor(...(isFinal ? [255,255,255] : PDF_COLORS.accentDark));
      docPdf.text(String(sfIdx + 1).padStart(2, '0'), M + chipSize/2, y + chipSize/2 + 1.3, { align: 'center' });
      // Nome da sub-ficha em peso 600
      docPdf.setFont('helvetica', 'bold').setFontSize(11).setTextColor(...PDF_COLORS.ink);
      docPdf.text(sf.name, M + chipSize + 3, y + chipSize/2 + 0.7);
      if (isFinal) {
        const nameWidth = docPdf.getTextWidth(sf.name);
        docPdf.setFont('helvetica', 'bold').setFontSize(6.5).setTextColor(...PDF_COLORS.accentDark);
        docPdf.setFillColor(...PDF_COLORS.accentLight);
        docPdf.roundedRect(M + chipSize + 3 + nameWidth + 3, y + 1.5, 18, 5, 1, 1, 'F');
        docPdf.text('FINAL', M + chipSize + 3 + nameWidth + 12, y + 5, { align: 'center' });
      }
      y += chipSize + 4;
      docPdf.setFont('helvetica', 'normal').setFontSize(8).setTextColor(...PDF_COLORS.muted);
      docPdf.text(`Rendimento ${sn.text} ${sn.unit}`.trim(), M + chipSize + 3, y);
      y += 5;

      // Tabela de ingredientes (sem custo)
      const ingRows = (sf.ingredientes || []).map(ing => {
        const f = formatIngQty(ing, sfScale);
        let nameLabel = ing.insumo_name || '';
        let subSf = null;
        const sfIdxInDish = dish.sub_fichas.findIndex(s => s.id === sf.id);
        if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
        if (!subSf && sfIdxInDish > 0) subSf = detectSubref(dish, sfIdxInDish, ing.insumo_name);
        if (subSf) nameLabel = `↪ ${nameLabel}`;
        else if (ing.variation_name) nameLabel = `${nameLabel} — ${ing.variation_name}`;
        return [nameLabel, f.text, f.unit, ing.observacao || ''];
      });
      if (ingRows.length > 0) {
        docPdf.autoTable({
          startY: y,
          margin: { left: M, right: M },
          head: [['Ingrediente', 'Qtd', 'Un.', 'Obs.']],
          body: ingRows,
          theme: 'plain',
          styles: { lineColor: PDF_COLORS.hairline, lineWidth: 0.1 },
          headStyles: {
            fillColor: PDF_COLORS.accentLight,
            textColor: PDF_COLORS.accent,
            fontStyle: 'bold',
            fontSize: 7.5,
            cellPadding: { top: 2.5, bottom: 2.5, left: 3, right: 3 }
          },
          bodyStyles: {
            fontSize: 9.5,
            cellPadding: { top: 2.2, bottom: 2.2, left: 3, right: 3 },
            textColor: PDF_COLORS.body
          },
          alternateRowStyles: { fillColor: PDF_COLORS.zebra },
          columnStyles: {
            1: { halign: 'right', cellWidth: 22, fontStyle: 'bold', textColor: PDF_COLORS.ink },
            2: { cellWidth: 14, textColor: PDF_COLORS.muted },
            3: { cellWidth: 55, textColor: PDF_COLORS.muted, fontSize: 8.5 }
          }
        });
        y = docPdf.lastAutoTable.finalY + 5;
      }

      // Modo de preparo (bloco discreto)
      if (sf.modo_preparo && sf.modo_preparo.trim()) {
        ensureSpace(15);
        docPdf.setFont('helvetica', 'bold').setFontSize(7).setTextColor(...PDF_COLORS.accentDark);
        docPdf.text('MODO DE PREPARO', M, y);
        y += 5;
        docPdf.setFont('helvetica', 'normal').setFontSize(9.5).setTextColor(...PDF_COLORS.body);
        const textWidth = W - 2 * M;
        const lines = docPdf.splitTextToSize(sf.modo_preparo, textWidth);
        lines.forEach(line => {
          ensureSpace(4.5);
          docPdf.text(line, M, y); y += 4.3;
        });
        y += 3;
      }
      y += 4;
    });
  });

  pdfDrawFooter(docPdf);
  docPdf.save(`roteiro-producao-${new Date().toISOString().slice(0, 10)}.pdf`);
  toast('PDF gerado');
}

// Modal: o usuário escolhe opções antes de gerar o PDF de Requisição
function openRequisicaoOptionsModal(cid) {
  const existing = $('#req-options-modal');
  if (existing) existing.remove();

  const groupCatChk = el('input', { type: 'checkbox' });
  const groupReutChk = el('input', { type: 'checkbox' });
  const withTimeChk = el('input', { type: 'checkbox' });
  const timeInput = el('input', { type: 'time', value: '09:00', disabled: '', class: 'req-time-input' });
  withTimeChk.addEventListener('change', () => {
    if (withTimeChk.checked) timeInput.removeAttribute('disabled');
    else timeInput.setAttribute('disabled', '');
  });

  const modal = el('div', { class: 'modal', id: 'req-options-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content' },
      el('h2', {}, 'Opções da Requisição'),
      el('p', { class: 'modal-subtitle' }, 'Personalize antes de gerar o PDF pro estoque.'),
      el('div', { class: 'req-options' },
        el('label', { class: 'req-opt' }, groupCatChk,
          el('div', {},
            el('strong', {}, 'Ordenar por categoria'),
            el('span', { class: 'muted' }, 'Agrupa insumos por tipo (carnes, vegetais, grãos, líquidos, etc.) — facilita pra quem busca no estoque.')
          )
        ),
        el('label', { class: 'req-opt' }, groupReutChk,
          el('div', {},
            el('strong', {}, 'Reutilizáveis em seção separada'),
            el('span', { class: 'muted' }, 'Move insumos marcados como "rateio" (papel-toalha, descartáveis, etc.) pra uma tabela separada no fim.')
          )
        ),
        el('label', { class: 'req-opt' }, withTimeChk,
          el('div', {},
            el('strong', {}, 'Hora estimada de retirada'),
            el('span', { class: 'muted' }, 'Inclui no cabeçalho a hora prevista pra entrega dos insumos no preparo.'),
            el('div', { style: 'margin-top:0.4rem;' }, timeInput)
          )
        )
      ),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: () => {
          const options = {
            groupByCategory: groupCatChk.checked,
            separateReutilizaveis: groupReutChk.checked,
            withPickupTime: withTimeChk.checked,
            pickupTime: timeInput.value
          };
          modal.remove();
          exportRequisicaoPDF(cid, options);
        } }, '↓ Gerar PDF')
      )
    )
  );
  document.body.appendChild(modal);
}

// Label legível das categorias de insumo
function categoryLabel(catId) {
  const c = INSUMO_CATEGORIES.find(x => x.id === catId);
  return c ? c.label : 'Outros';
}

// PDF de Requisição: lista pro estoquista entregar os insumos
function exportRequisicaoPDF(cid, options) {
  options = options || {};
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const M = PDF_LAYOUT.margin;
  const W = PDF_LAYOUT.pageWidth;
  const H = PDF_LAYOUT.pageHeight;
  let y;

  // ── Cabeçalho ──
  y = pdfDrawHeader(docPdf, 'Requisição de Estoque', 'Pedido de insumos para a produção');

  // Linha de meta: solicitante + retirada
  docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(...PDF_COLORS.muted);
  docPdf.text('Solicitante', M, y);
  docPdf.text('Estoquista', M + 70, y);
  if (options.withPickupTime && options.pickupTime) {
    docPdf.setFont('helvetica', 'bold').setFontSize(9).setTextColor(...PDF_COLORS.accent);
    docPdf.text(`Retirar até ${options.pickupTime}`, W - M, y, { align: 'right' });
  }
  y += 4;
  // Linhas pra preencher
  docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
  docPdf.line(M, y, M + 60, y);
  docPdf.line(M + 70, y, M + 130, y);
  y += 8;

  // ── Resumo da produção — card com border ──
  const resumo = buildProdResumoLines();
  if (resumo.length > 0) {
    const card_h = 9 + resumo.length * 5;
    docPdf.setFillColor(...PDF_COLORS.zebra);
    docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
    docPdf.roundedRect(M, y, W - 2 * M, card_h, 1.5, 1.5, 'FD');
    docPdf.setFont('helvetica', 'bold').setFontSize(7.5).setTextColor(...PDF_COLORS.accentDark);
    docPdf.text('PARA A PRODUÇÃO DE', M + 4, y + 5);
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.ink);
    resumo.forEach((r, i) => {
      docPdf.text(r.name, M + 4, y + 10 + i * 5);
      docPdf.setFont('helvetica', 'bold').setTextColor(...PDF_COLORS.body);
      docPdf.text(`${fmtNum(r.qty, 0)} ${r.unit}`, W - M - 4, y + 10 + i * 5, { align: 'right' });
      docPdf.setFont('helvetica', 'normal').setTextColor(...PDF_COLORS.ink);
    });
    y += card_h + 8;
  }

  const shoppingRaw = (window.__prodBuildShopping ? window.__prodBuildShopping() : []);
  if (shoppingRaw.length === 0) {
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(...PDF_COLORS.body);
    docPdf.text('Nenhum insumo a solicitar.', M, y);
    pdfDrawFooter(docPdf);
    docPdf.save(`requisicao-${new Date().toISOString().slice(0, 10)}.pdf`);
    return;
  }

  const normais = options.separateReutilizaveis ? shoppingRaw.filter(r => !r.isReutilizavel) : shoppingRaw.slice();
  const reut = options.separateReutilizaveis ? shoppingRaw.filter(r => r.isReutilizavel) : [];

  function renderTable(title, rows, startN) {
    if (rows.length === 0) return startN;
    if (y > H - 50) { docPdf.addPage(); y = M; }
    // Header de seção
    docPdf.setFont('helvetica', 'bold').setFontSize(7.5).setTextColor(...PDF_COLORS.accentDark);
    docPdf.text(title.toUpperCase() + ` · ${rows.length}`, M, y); y += 2;
    docPdf.setDrawColor(...PDF_COLORS.accent).setLineWidth(0.5);
    docPdf.line(M, y, M + 20, y); y += 5;

    docPdf.autoTable({
      startY: y,
      margin: { left: M, right: M },
      head: [['#', 'Insumo', 'Qtd', 'Un.', 'Retirado', 'Visto']],
      body: rows.map((r, i) => {
        const norm = normUnitForDisplay(r.qty, r.unit);
        const label = r.isReutilizavel ? `${r.name}  (rateio)` : r.name;
        return [String(startN + i), label, norm.text, norm.unit, '', ''];
      }),
      theme: 'plain',
      styles: { lineColor: PDF_COLORS.hairline, lineWidth: 0.1 },
      headStyles: {
        fillColor: PDF_COLORS.accentLight,
        textColor: PDF_COLORS.accentDark,
        fontStyle: 'bold',
        fontSize: 7,
        cellPadding: { top: 2.5, bottom: 2.5, left: 3, right: 3 }
      },
      bodyStyles: {
        fontSize: 9.5,
        cellPadding: { top: 3, bottom: 3, left: 3, right: 3 },
        textColor: PDF_COLORS.body,
        minCellHeight: 9
      },
      alternateRowStyles: { fillColor: PDF_COLORS.zebra },
      columnStyles: {
        0: { cellWidth: 10, halign: 'center', textColor: PDF_COLORS.subtle, fontSize: 8 },
        2: { cellWidth: 22, halign: 'right', fontStyle: 'bold', textColor: PDF_COLORS.ink },
        3: { cellWidth: 14, textColor: PDF_COLORS.muted },
        4: { cellWidth: 26, halign: 'right' },
        5: { cellWidth: 26 }
      },
      didDrawPage: (data) => { y = data.cursor.y; }
    });
    y = docPdf.lastAutoTable.finalY + 7;
    return startN + rows.length;
  }

  let counter = 1;
  if (options.groupByCategory) {
    const byCat = {};
    normais.forEach(r => {
      const cat = categorizeInsumo({ name: r.name });
      if (!byCat[cat]) byCat[cat] = [];
      byCat[cat].push(r);
    });
    const catIds = Object.keys(byCat).sort((a, b) => {
      if (a === 'outros') return 1;
      if (b === 'outros') return -1;
      return categoryLabel(a).localeCompare(categoryLabel(b), 'pt-BR');
    });
    catIds.forEach(catId => {
      const rows = byCat[catId].sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));
      counter = renderTable(categoryLabel(catId), rows, counter);
    });
  } else {
    const sorted = normais.slice().sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));
    counter = renderTable('Insumos solicitados', sorted, counter);
  }

  if (reut.length > 0) {
    const sorted = reut.slice().sort((a, b) => a.name.localeCompare(b.name, 'pt-BR'));
    counter = renderTable('Reutilizáveis / rateio', sorted, counter);
  }

  // ── Bloco de assinaturas no final ──
  if (y > H - 40) { docPdf.addPage(); y = M; }
  y += 8;
  docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.2);
  docPdf.line(M, y, W - M, y); y += 8;
  // 3 colunas: Aprovado / Estoquista / Hora real
  const colW = (W - 2 * M) / 3;
  ['Aprovado por', 'Entregue por (estoquista)', 'Data e hora da entrega'].forEach((label, i) => {
    const cx = M + i * colW;
    docPdf.setDrawColor(...PDF_COLORS.hairline).setLineWidth(0.3);
    docPdf.line(cx + 2, y, cx + colW - 4, y);
    docPdf.setFont('helvetica', 'normal').setFontSize(7).setTextColor(...PDF_COLORS.muted);
    docPdf.text(label, cx + 2, y + 4);
  });

  pdfDrawFooter(docPdf);
  docPdf.save(`requisicao-${new Date().toISOString().slice(0, 10)}.pdf`);
  toast('PDF gerado');
}

function exportProducaoXLSX(cid) {
  const XLSX = window.XLSX;
  const wb = XLSX.utils.book_new();
  const cliente = STATE.currentCliente;
  const hoje = new Date().toLocaleDateString('pt-BR');

  // Resumo
  const resumoData = [[`Plano de produção — ${cliente?.name || cid}`], [`Data: ${hoje}`], [],
    ['Prato', 'Qty alvo', 'Unidade', 'Escala', 'Custo est.']];
  PROD_PLAN.items.forEach(item => {
    const dish = STATE.dishes.find(d => d.id === item.dishId);
    if (!dish) return;
    const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
    const origRend = getSfRendimento(finalSf);
    let scale = 1;
    if (item.targetQty > 0 && origRend.qty > 0) {
      const [qConv] = normalizeSubrefQty(item.targetQty, item.targetUnit, origRend.unit);
      scale = qConv / origRend.qty;
    }
    const cost = dishCost(dish).total * scale;
    resumoData.push([dish.name, item.targetQty, item.targetUnit, scale, cost]);
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(resumoData), 'Resumo');

  // Lista de compras
  const shopping = window.__prodBuildShopping ? window.__prodBuildShopping() : [];
  const shopData = [['Lista de compras consolidada'], [], ['Insumo', 'Qty', 'Unidade', 'Rateio?']];
  shopping.forEach(r => {
    const norm = normUnitForDisplay(r.qty, r.unit);
    shopData.push([r.name, norm.text, norm.unit, r.isReutilizavel ? 'SIM' : '']);
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(shopData), 'Compras');

  // Requisição
  const reqData = [[`REQUISIÇÃO DE ESTOQUE — ${cliente?.name || cid}`], [`Data: ${hoje}`], [`Responsável: ____________________`], [],
    ['Insumo', 'Qty solicitada', 'Unidade', 'Qty retirada', 'Assinatura']];
  shopping.forEach(r => {
    const norm = normUnitForDisplay(r.qty, r.unit);
    reqData.push([r.name, norm.text, norm.unit, '', '']);
  });
  XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(reqData), 'Requisição');

  XLSX.writeFile(wb, `producao-${new Date().toISOString().slice(0, 10)}.xlsx`);
  toast('Excel gerado');
}

function renderInsumos(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  const canEdit = canEditInsumoPrice(cid);
  const canFullEdit = canEditCliente(cid);
  const header = el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Insumos'),
      el('p', {}, canEdit
        ? 'Atualize os preços — salva automaticamente e todas as fichas recalculam.'
        : 'Preços dos insumos.')
    ),
    el('div', { class: 'insumos-actions' },
      el('input', { type: 'search', class: 'insumos-search', placeholder: 'Buscar insumo…' }),
      canFullEdit ? el('button', { class: 'btn btn-small', title: 'Remove insumos que não são usados em nenhuma ficha e insumos que são subprodutos (caldos gerados por sub-fichas etc.)', onclick: async () => {
        try {
          const n = await cleanupOrphanInsumos(cid);
          toast(n > 0 ? `${n} insumo${n > 1 ? 's' : ''} removido${n > 1 ? 's' : ''}` : 'Nenhum insumo órfão encontrado');
        } catch (err) { toast('Erro: ' + err.message); }
      } }, '↻ Limpar órfãos') : null,
      canFullEdit ? el('button', { class: 'btn btn-primary btn-small', onclick: () => openNovoInsumoModal(cid) }, '+ Novo insumo') : null
    )
  );
  app.appendChild(header);

  // Filter chips por categoria
  const allCats = [{ id: 'all', label: 'Todos' }, ...INSUMO_CATEGORIES, { id: 'outros', label: 'Outros' }];
  const filterState = { category: 'all', search: '' };
  const filterBar = el('div', { class: 'insumo-filter-chips' });
  allCats.forEach(c => {
    const btn = el('button', { class: 'chip-filter' + (c.id === 'all' ? ' active' : ''), 'data-cat': c.id }, c.label);
    filterBar.appendChild(btn);
  });
  app.appendChild(filterBar);

  const tbl = el('table', { class: 'insumos-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', {}, 'Unidade'),
      el('th', {}, 'Preço (R$)'),
      el('th', { title: 'Insumos reutilizáveis (óleo de fritura, etc) não entram no custo do prato — devem ir em despesas operacionais' }, 'Rateio'),
      el('th', {}, 'Variações'),
      el('th', {}, 'Usado em')
    ))
  );
  const tbody = el('tbody', {});
  // Usa Set pra agregar nomes únicos de pratos por insumo_id (não conta 2x se o insumo aparece em 2 sub-fichas do mesmo prato)
  const usageMap = {};
  STATE.dishes.forEach(d => (d.sub_fichas || []).forEach(sf =>
    (sf.ingredientes || []).forEach(i => {
      if (!i.insumo_id || i.subref_id) return;
      if (!usageMap[i.insumo_id]) usageMap[i.insumo_id] = new Set();
      usageMap[i.insumo_id].add(d.name);
    })
  ));
  function buildRows() {
    tbody.innerHTML = '';
    const f = filterState.search.toLowerCase().trim();
    const cat = filterState.category;
    STATE.insumos
      .filter(i => !f || i.name.toLowerCase().includes(f))
      .filter(i => cat === 'all' || categorizeInsumo(i) === cat)
      .forEach(insumo => {
        const unitInput = buildUnitSelect(insumo.unit || 'kg');
        const priceInput = el('input', { type: 'number', min: '0', step: '0.01', value: insumo.price || 0 });
        if (!canEdit) { unitInput.disabled = true; priceInput.disabled = true; }
        unitInput.addEventListener('change', () => {
          const oldUnit = insumo.unit || '';
          const newUnit = unitInput.value;
          if (oldUnit && unitCategory(oldUnit) !== unitCategory(newUnit)) {
            if (!confirm(`Trocar de "${oldUnit}" para "${newUnit}" muda a categoria da unidade. O preço por ${newUnit} pode estar errado — revise após salvar. Continuar?`)) {
              unitInput.value = oldUnit;
              return;
            }
            toast('⚠ Categoria da unidade mudou — revise o preço');
          }
          insumo.unit = newUnit;
          scheduleSave('ins-' + insumo.id, () => saveInsumo(cid, insumo));
        });
        priceInput.addEventListener('input', () => {
          insumo.price = parseFloat(priceInput.value) || 0;
          scheduleSave('ins-' + insumo.id, () => saveInsumo(cid, insumo));
        });
        const varCount = (insumo.variations || []).length;
        const varBtn = el('button', {
          class: 'btn btn-small' + (varCount > 0 ? ' btn-primary' : ''),
          onclick: () => openVariationsModal(cid, insumo)
        }, varCount > 0 ? `${varCount} variação${varCount > 1 ? 'ões' : ''}` : '+ adicionar');
        if (!canFullEdit) varBtn.disabled = true;
        const reutInput = el('input', { type: 'checkbox', class: 'reut-check', title: 'Insumo reutilizável — não entra no custo do prato' });
        if (insumo.reutilizavel) reutInput.setAttribute('checked', '');
        if (!canFullEdit) reutInput.disabled = true;
        reutInput.addEventListener('change', () => {
          if (reutInput.checked) {
            insumo.reutilizavel = true;
          } else {
            delete insumo.reutilizavel;
          }
          scheduleSave('ins-' + insumo.id, () => saveInsumo(cid, insumo));
        });
        const usedIn = usageMap[insumo.id] ? Array.from(usageMap[insumo.id]).sort() : [];
        const usageTooltip = el('div', { class: 'usage-tooltip' },
          el('strong', {}, 'Usado em:'),
          ...usedIn.map(name => el('div', { class: 'usage-tooltip-item' }, name))
        );
        const usageCell = el('td', {
          'data-label': 'Usado em',
          class: 'usage-cell' + (usedIn.length > 0 ? ' has-usage' : ''),
        },
          el('span', { class: 'usage-count' }, usedIn.length + ' receita' + (usedIn.length === 1 ? '' : 's')),
          usedIn.length > 0 ? usageTooltip : null
        );
        const cells = [
          el('td', { 'data-label': 'Insumo' }, insumo.name, insumo.reutilizavel ? el('span', { class: 'reut-badge-inline' }, 'rateio') : null),
          el('td', { 'data-label': 'Unidade' }, unitInput),
          el('td', { 'data-label': 'Preço (R$)' }, priceInput),
          el('td', { 'data-label': 'Rateio', class: 'reut-cell' }, reutInput),
          el('td', { 'data-label': 'Variações' }, varBtn),
          usageCell,
        ];
        tbody.appendChild(el('tr', { class: insumo.reutilizavel ? 'is-reut-row' : '' }, ...cells));
      });
  }
  buildRows();
  tbl.appendChild(tbody);
  const tableWrap = el('div', { class: 'insumos-table-wrap' }, tbl);
  app.appendChild(tableWrap);
  $('.insumos-search', header).addEventListener('input', e => {
    filterState.search = e.target.value;
    buildRows();
  });
  filterBar.addEventListener('click', e => {
    const b = e.target.closest('.chip-filter');
    if (!b) return;
    filterState.category = b.dataset.cat;
    $$('.chip-filter', filterBar).forEach(x => x.classList.toggle('active', x === b));
    buildRows();
  });
}

// Modal pra copiar sub-ficha de outra receita do mesmo restaurante
function openCopySubfichaModal(targetDish, onCopy) {
  const modal = el('div', { class: 'modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() })
  );
  const content = el('div', { class: 'modal-content modal-wide' });
  content.appendChild(el('h2', {}, 'Copiar sub-ficha de outra ficha'));
  content.appendChild(el('p', { class: 'modal-subtitle' },
    'Selecione uma sub-ficha existente pra copiar pra esta ficha. ' +
    'Ingredientes, rendimento, modo de preparo e variações serão copiados. ' +
    'Referências a outras sub-fichas (subref) serão removidas porque elas não existem nesta ficha.'));

  // Lista de fichas (exceto a atual) com sub-fichas navegáveis
  const listWrap = el('div', { class: 'copy-sf-list' });
  const searchInput = el('input', { type: 'search', placeholder: 'Buscar ficha ou sub-ficha...', class: 'copy-sf-search' });
  content.appendChild(searchInput);

  function renderList() {
    listWrap.innerHTML = '';
    const q = searchInput.value.toLowerCase().trim();
    let totalShown = 0;
    STATE.dishes.forEach(dish => {
      if (dish.id === targetDish.id) return;
      if (!dish.sub_fichas || dish.sub_fichas.length === 0) return;
      const matchingSfs = dish.sub_fichas.filter(sf =>
        !q ||
        dish.name.toLowerCase().includes(q) ||
        sf.name.toLowerCase().includes(q)
      );
      if (matchingSfs.length === 0) return;
      const dishGroup = el('div', { class: 'copy-sf-dish' },
        el('h4', { class: 'copy-sf-dish-name' }, dish.name)
      );
      matchingSfs.forEach(sf => {
        const ingCount = (sf.ingredientes || []).length;
        const rend = sf.rendimento || '—';
        const btn = el('button', { class: 'copy-sf-item', onclick: () => {
          const copy = JSON.parse(JSON.stringify(sf));
          copy.id = uid();
          // Prefixo no nome pra evitar duplicata idêntica
          const existingNames = targetDish.sub_fichas.map(x => x.name);
          if (existingNames.includes(copy.name)) {
            copy.name = copy.name + ' (cópia)';
          }
          // Remove subref_id de ingredientes (não existem na ficha destino)
          let subrefsCleared = 0;
          (copy.ingredientes || []).forEach(ing => {
            if (ing.subref_id) { delete ing.subref_id; subrefsCleared++; }
          });
          if (subrefsCleared > 0) {
            toast(`${subrefsCleared} referência${subrefsCleared > 1 ? 's' : ''} a outras sub-fichas removida${subrefsCleared > 1 ? 's' : ''}`);
          }
          onCopy(copy);
          modal.remove();
        } },
          el('div', { class: 'copy-sf-item-name' }, sf.name),
          el('div', { class: 'copy-sf-item-meta' }, `${ingCount} ingredientes · rend. ${rend}`)
        );
        dishGroup.appendChild(btn);
        totalShown++;
      });
      listWrap.appendChild(dishGroup);
    });
    if (totalShown === 0) {
      listWrap.appendChild(el('p', { class: 'muted' }, q ? 'Nenhuma sub-ficha encontrada com essa busca.' : 'Não há outras fichas com sub-fichas neste restaurante.'));
    }
  }
  renderList();
  searchInput.addEventListener('input', renderList);
  content.appendChild(listWrap);
  content.appendChild(el('div', { class: 'modal-actions' },
    el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar')
  ));
  modal.appendChild(content);
  document.body.appendChild(modal);
  searchInput.focus();
}

// Modal de variações do insumo (ex: Cebola branca → brunoise FC 1,15, julienne FC 1,12)
function openVariationsModal(cid, insumo) {
  const variations = JSON.parse(JSON.stringify(insumo.variations || []));
  const original = JSON.stringify(variations);
  const modal = el('div', { class: 'modal' },
    el('div', { class: 'modal-overlay', onclick: () => {
      if (JSON.stringify(variations) !== original && !confirm('Há alterações não salvas. Descartar?')) return;
      modal.remove();
    } })
  );
  const content = el('div', { class: 'modal-content modal-wide' },
    el('h2', {}, 'Variações de ' + insumo.name),
    el('p', { class: 'modal-subtitle' },
      'Variações são cortes ou preparos do insumo base (ex: brunoise, julienne) com fator de correção próprio. ' +
      'Nas fichas, selecionar uma variação carrega o FC automaticamente. Na lista de compras, agrega no insumo base.')
  );
  const dirtyHint = el('p', { class: 'dirty-hint', style: 'display:none;' }, '● Alterações não salvas — clique em "Salvar" pra confirmar.');
  content.appendChild(dirtyHint);
  let saveBtn;
  function markDirty() {
    const isDirty = JSON.stringify(variations) !== original;
    dirtyHint.style.display = isDirty ? '' : 'none';
    if (saveBtn) saveBtn.classList.toggle('btn-dirty', isDirty);
  }
  const varList = el('div', { class: 'variations-list' });
  function renderVars() {
    varList.innerHTML = '';
    if (variations.length === 0) {
      varList.appendChild(el('p', { class: 'muted empty-variations' }, 'Nenhuma variação cadastrada. Clique em "+ Adicionar variação" abaixo.'));
    }
    variations.forEach((v, idx) => {
      const nameI = el('input', { type: 'text', value: v.name || '', placeholder: 'Ex: brunoise' });
      nameI.addEventListener('input', () => { v.name = nameI.value; markDirty(); });
      const fcI = el('input', { type: 'text', inputmode: 'decimal', value: v.fc != null ? String(v.fc).replace('.', ',') : '', placeholder: 'Ex: 1,15' });
      fcI.addEventListener('input', () => {
        const n = parseFloat(fcI.value.replace(',', '.'));
        v.fc = isNaN(n) ? null : n;
        markDirty();
      });
      const delBtn = el('button', { class: 'btn btn-small btn-danger', onclick: () => {
        variations.splice(idx, 1); renderVars(); markDirty();
      } }, '×');
      varList.appendChild(el('div', { class: 'variation-row' },
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameI),
        el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'FC'), fcI),
        delBtn
      ));
    });
  }
  renderVars();
  content.appendChild(varList);
  content.appendChild(el('button', { class: 'btn btn-small', onclick: () => {
    variations.push({ name: '', fc: 1 });
    renderVars();
    markDirty();
  } }, '+ Adicionar variação'));
  saveBtn = el('button', { class: 'btn btn-primary', onclick: async () => {
    // Valida: nomes únicos, FC válido
    const clean = variations
      .filter(v => (v.name || '').trim())
      .map(v => ({ name: v.name.trim(), fc: (typeof v.fc === 'number' && v.fc > 0) ? v.fc : 1 }));
    const names = new Set();
    for (const v of clean) {
      if (names.has(v.name.toLowerCase())) { alert(`Nome duplicado: "${v.name}"`); return; }
      names.add(v.name.toLowerCase());
    }
    try {
      if (clean.length > 0) {
        insumo.variations = clean;
        await saveInsumo(cid, insumo);
      } else {
        // Removeu todas: precisa apagar campo no Firestore (merge:true não apaga ausentes)
        delete insumo.variations;
        await setDoc(insumoDoc(cid, insumo.id), { variations: deleteField() }, { merge: true });
      }
      toast('Variações salvas');
      modal.remove();
    } catch (err) { toast('Erro: ' + err.message); }
  } }, 'Salvar');
  content.appendChild(el('div', { class: 'modal-actions' },
    el('button', { class: 'btn', onclick: () => {
      if (JSON.stringify(variations) !== original && !confirm('Há alterações não salvas. Descartar?')) return;
      modal.remove();
    } }, 'Cancelar'),
    saveBtn
  ));
  modal.appendChild(content);
  document.body.appendChild(modal);
}

function openNovoInsumoModal(cid) {
  const modal = el('div', { class: 'modal', id: 'novo-insumo-modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() }),
    el('div', { class: 'modal-content' },
      el('h2', {}, 'Novo insumo'),
      el('p', { class: 'modal-subtitle' }, 'Adiciona um novo insumo à lista deste cliente'),
      (() => {
        const nameInput = el('input', { type: 'text', placeholder: 'Ex: Farinha de Arroz' });
        const unitInput = buildUnitSelect('kg');
        const priceInput = el('input', { type: 'number', min: '0', step: '0.01', placeholder: '0,00' });
        const reutInput = el('input', { type: 'checkbox' });
        const formEl = el('div', { class: 'form-grid' },
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Unidade'), unitInput),
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Preço (R$)'), priceInput),
          el('label', { class: 'field field-inline' }, reutInput,
            el('span', { class: 'label-text', style: 'margin-left:0.5rem;' },
              'Reutilizável (óleo de fritura, gás etc.) — não entra no custo do prato'))
        );
        modal.__submit = async () => {
          const name = nameInput.value.trim();
          const unit = unitInput.value || 'kg';
          const price = parseFloat(priceInput.value) || 0;
          if (!name) { alert('Nome obrigatório'); return; }
          const id = slugify(name);
          if (STATE.insumos.find(i => i.id === id)) { alert('Já existe um insumo com esse nome'); return; }
          try {
            const payload = { id, name, unit, price };
            if (reutInput.checked) payload.reutilizavel = true;
            await saveInsumo(cid, payload);
            toast('Insumo criado');
            modal.remove();
          } catch (err) { toast('Erro: ' + err.message); }
        };
        return formEl;
      })(),
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: () => modal.__submit() }, 'Criar')
      )
    )
  );
  document.body.appendChild(modal);
}

// ---------- Views: Admin dishes ----------
function renderAdminList(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  const isFullAdmin = canEditCliente(cid);
  const header = el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Gerenciar fichas'),
      el('p', {}, isFullAdmin
        ? 'Adicione, edite ou exclua pratos. Salva automaticamente.'
        : 'Crie fichas próprias ou edite as que estão desbloqueadas. Fichas bloqueadas pela consultoria só podem ser visualizadas.')
    ),
    el('a', { class: 'btn btn-primary', href: `#/c/${cid}/admin/new` }, '+ Nova ficha')
  );
  app.appendChild(header);
  const panel = el('div', { class: 'admin-panel' });
  const list = el('div', { class: 'dish-admin-list' });
  STATE.dishes.forEach(dish => {
    const canEdit = canEditDish(cid, dish);
    const lockedByConsultor = !!dish.locked && !isFullAdmin;
    const item = el('div', { class: 'dish-admin-item' + (lockedByConsultor ? ' dish-locked' : '') },
      el('div', { class: 'info' },
        el('h4', {}, dish.name,
          dish.locked && isFullAdmin ? el('span', { class: 'lock-badge lock-by-master', title: 'Bloqueada pra edição pelo cliente' }, '🔒 travada') : null,
          lockedByConsultor ? el('span', { class: 'lock-badge lock-by-consultor', title: 'Bloqueada para edição pela consultoria' }, '🔒 bloqueada pelo consultor') : null
        ),
        el('p', {}, `${(dish.sub_fichas || []).length} sub-fichas · ${dish.photos?.length || 0} fotos`)
      ),
      el('div', { class: 'dish-admin-actions' },
        el('a', { class: 'btn btn-small', href: `#/c/${cid}/ficha/${dish.id}` }, 'Ver'),
        canEdit
          ? el('a', { class: 'btn btn-small btn-primary', href: `#/c/${cid}/admin/edit/${dish.id}` }, 'Editar')
          : el('button', { class: 'btn btn-small', disabled: '', title: 'Esta ficha foi bloqueada pela consultoria' }, 'Editar 🔒'),
        isFullAdmin
          ? el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
              if (!confirm(`Excluir "${dish.name}"?`)) return;
              try { await deleteDish(cid, dish.id); toast('Excluída'); } catch (e) { toast('Erro: ' + e.message); }
            } }, 'Excluir')
          : null
      )
    );
    list.appendChild(item);
  });
  panel.appendChild(list);
  app.appendChild(panel);
}

// Cliente (dono) gerencia a própria equipe (admin + op) do restaurante atual
async function renderClientTeamAdmin(cid) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  renderLoadingScreen();
  const allUsersSnap = await getDocs(collection(db, 'users'));
  const teamUsers = allUsersSnap.docs
    .map(d => ({ uid: d.id, ...d.data() }))
    .filter(u => (u.clienteIds || []).includes(cid))
    .filter(u => u.role === 'cliente_admin' || u.role === 'cliente_op' || u.role === 'equipe');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  app.appendChild(el('div', { class: 'page-header' },
    el('div', {},
      el('h1', {}, 'Equipe do restaurante'),
      el('p', {}, 'Cadastre as pessoas que ajudam você a operar a cozinha. Equipe Admin vê preços e planeja produção; Equipe Operacional só acessa fichas e produção.')
    )
  ));
  // Painel de convite
  const invitePanel = el('section', { class: 'admin-panel-card' },
    el('h3', {}, 'Adicionar pessoa à equipe'),
    el('p', { class: 'muted' }, 'Após salvar, o sistema gera uma senha temporária — copie ou envie pelo WhatsApp pra pessoa. No primeiro login ela troca por uma senha pessoal.')
  );
  const nameInput = el('input', { type: 'text', placeholder: 'Nome completo' });
  const emailInput = el('input', { type: 'email', placeholder: 'email@exemplo.com' });
  const whatsInput = el('input', { type: 'tel', placeholder: '(11) 91234-5678' });
  const roleSelect = el('select', {},
    el('option', { value: 'cliente_op' }, 'Equipe Operacional — acessa fichas e produção'),
    el('option', { value: 'cliente_admin' }, 'Equipe Admin — vê preços, planeja produção')
  );
  invitePanel.appendChild(el('div', { class: 'form-grid' },
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Email'), emailInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'WhatsApp'), whatsInput),
    el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Papel'), roleSelect)
  ));
  invitePanel.appendChild(el('div', { class: 'panel-actions' },
    el('button', { class: 'btn btn-primary', onclick: async () => {
      const email = emailInput.value.trim();
      const name = nameInput.value.trim() || email.split('@')[0];
      if (!name) { alert('Informe o nome'); return; }
      if (!email) { alert('Informe o email'); return; }
      try {
        await inviteUser(email, name, roleSelect.value, [cid], whatsInput.value.trim());
      } catch (err) { alert('Erro: ' + (err.message || err.code)); }
      renderClientTeamAdmin(cid);
    } }, 'Adicionar à equipe')
  ));
  app.appendChild(invitePanel);

  // Lista
  app.appendChild(el('h3', { class: 'section-title' }, `Equipe atual (${teamUsers.length})`));
  if (teamUsers.length === 0) {
    app.appendChild(el('div', { class: 'empty-state' }, el('p', { class: 'muted' }, 'Ainda não há ninguém na equipe.')));
    return;
  }
  const grid = el('div', { class: 'user-admin-list' });
  teamUsers.forEach(u => {
    const isMe = u.uid === STATE.user?.uid;
    const card = el('article', { class: 'user-card' + (u.disabled ? ' user-card-disabled' : '') });
    card.appendChild(el('div', { class: 'user-card-head' },
      el('div', { class: 'user-id' },
        el('h4', { class: 'user-name' }, u.name || '—',
          u.disabled ? el('span', { class: 'disabled-badge' }, '⏸ inativo') : null,
          !u.disabled && u.mustChangePassword ? el('span', { class: 'pending-badge' }, '⏳ aguarda 1º login') : null
        ),
        el('span', { class: 'user-email' }, u.email),
        u.whatsapp ? el('span', { class: 'user-whatsapp' }, '📱 ' + u.whatsapp) : null
      ),
      el('span', { class: 'role-badge role-' + (u.role || 'none') }, roleLabel(u.role))
    ));
    const actions = el('div', { class: 'user-actions' });
    if (isMe) actions.appendChild(el('span', { class: 'muted' }, '(você)'));
    else if (u.disabled) {
      actions.appendChild(el('button', { class: 'btn btn-small btn-primary', onclick: async () => {
        try {
          await setDoc(doc(db, 'users', u.uid), { disabled: deleteField(), disabledAt: deleteField(), reactivatedAt: new Date().toISOString() }, { merge: true });
          toast('Reativado');
          renderClientTeamAdmin(cid);
        } catch (err) { alert('Erro: ' + (err.message || err.code)); }
      } }, '↻ Reativar'));
    } else {
      // Toggle entre admin e op
      const otherRole = u.role === 'cliente_admin' ? 'cliente_op' : 'cliente_admin';
      const otherLabel = otherRole === 'cliente_admin' ? 'Promover a Admin' : 'Rebaixar para Operacional';
      actions.appendChild(el('button', { class: 'btn btn-small', onclick: async () => {
        if (!confirm(`Mudar ${u.email} para "${roleLabel(otherRole)}"?`)) return;
        try {
          await setDoc(doc(db, 'users', u.uid), { role: otherRole }, { merge: true });
          toast('Papel alterado');
          renderClientTeamAdmin(cid);
        } catch (err) { alert('Erro: ' + (err.message || err.code)); }
      } }, otherLabel));
      actions.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: async () => {
        if (!confirm(`Desativar ${u.email}? Pode reativar depois.`)) return;
        try {
          await setDoc(doc(db, 'users', u.uid), { disabled: true, disabledAt: new Date().toISOString() }, { merge: true });
          toast('Desativado');
          renderClientTeamAdmin(cid);
        } catch (err) { alert('Erro: ' + (err.message || err.code)); }
      } }, 'Desativar'));
    }
    card.appendChild(actions);
    grid.appendChild(card);
  });
  app.appendChild(grid);
}

function renderAdminEdit(cid, dishId) {
  const app = $('#app');
  app.innerHTML = '';
  app.appendChild(renderClienteContext(cid));
  let dish;
  if (dishId) {
    const existing = STATE.dishes.find(d => d.id === dishId);
    if (!existing) { app.appendChild(el('p', {}, 'Prato não encontrado')); return; }
    // Bloqueia cliente ao tentar editar ficha locked
    if (!canEditDish(cid, existing)) {
      app.appendChild(el('a', { href: `#/c/${cid}/admin`, class: 'back-link' }, '← Voltar'));
      app.appendChild(el('div', { class: 'trial-block' },
        el('div', { class: 'trial-block-card' },
          el('h1', {}, 'Ficha bloqueada'),
          el('p', {}, `A ficha "${existing.name}" está bloqueada para edição pela consultoria.`),
          el('p', { class: 'muted' }, 'Você pode visualizá-la normalmente, mas alterações precisam ser feitas pelo consultor.'),
          el('a', { class: 'btn', href: `#/c/${cid}/ficha/${existing.id}` }, 'Ver ficha'),
          el('a', { class: 'btn', href: `#/c/${cid}/admin`, style: 'margin-left:0.5rem;' }, 'Voltar')
        )
      ));
      return;
    }
    dish = JSON.parse(JSON.stringify(existing));
  } else {
    // Nova ficha: master/staff cria locked por default; cliente cria desbloqueada
    const defaultLocked = isMaster() || isStaff();
    dish = { id: 'new-' + uid(), name: '', description: '', photos: [], louca: '', equipamentos: [],
      sub_fichas: [{ id: uid(), name: 'Preparação 1', rendimento: '', ingredientes: [], modo_preparo: '' }],
      markup: 300,
      locked: defaultLocked,
      createdBy: isClienteUser() ? 'cliente' : 'consultoria'
    };
  }
  app.appendChild(el('a', { href: `#/c/${cid}/admin`, class: 'back-link' }, '← Voltar'));
  app.appendChild(el('h1', {}, dishId ? 'Editar ficha' : 'Nova ficha'));
  const panel = el('div', { class: 'admin-panel' });
  // Switch de bloqueio (só master/staff vê)
  if (canToggleDishLock(cid)) {
    const lockToggle = el('input', { type: 'checkbox' });
    if (dish.locked) lockToggle.setAttribute('checked', '');
    lockToggle.addEventListener('change', () => { dish.locked = lockToggle.checked; });
    panel.appendChild(el('div', { class: 'lock-toggle-box' },
      lockToggle,
      el('div', {},
        el('strong', {}, '🔒 Bloquear edição pelo cliente'),
        el('p', { class: 'muted' }, 'Quando ativo, o dono do restaurante e a equipe dele não conseguem editar esta ficha — só visualizar. Use pra fichas que você entrega como parte da consultoria e quer manter o controle.')
      )
    ));
  }
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
  function renderEquipamentos() {
    eqBox.innerHTML = '';
    (STATE.equipamentos || []).forEach(eq => {
      const i = el('input', { type: 'checkbox' });
      if (dish.equipamentos.includes(eq)) i.setAttribute('checked', '');
      i.addEventListener('change', () => {
        if (i.checked) { if (!dish.equipamentos.includes(eq)) dish.equipamentos.push(eq); }
        else dish.equipamentos = dish.equipamentos.filter(x => x !== eq);
      });
      const removeBtn = el('button', {
        class: 'eq-remove',
        title: 'Remover equipamento da lista',
        onclick: async (e) => {
          e.preventDefault();
          if (!confirm(`Remover "${eq}" da lista de equipamentos deste restaurante?`)) return;
          const newList = (STATE.equipamentos || []).filter(x => x !== eq);
          try {
            await setDoc(configDoc(cid, 'equipamentos'), { list: newList });
            dish.equipamentos = dish.equipamentos.filter(x => x !== eq);
            toast('Equipamento removido');
          } catch (err) { toast('Erro: ' + err.message); }
        }
      }, '×');
      eqBox.appendChild(el('label', { class: 'equipamento-item' }, i, document.createTextNode(' ' + eq), removeBtn));
    });
  }
  renderEquipamentos();
  panel.appendChild(eqBox);
  // Botão adicionar novo equipamento
  const addEqRow = el('div', { class: 'add-equipamento-row' });
  const newEqInput = el('input', { type: 'text', placeholder: 'Novo equipamento (ex: Sous-vide)', class: 'add-eq-input' });
  const addEqBtn = el('button', { class: 'btn btn-small', onclick: async (e) => {
    e.preventDefault();
    const name = newEqInput.value.trim();
    if (!name) return;
    if ((STATE.equipamentos || []).some(x => x.toLowerCase() === name.toLowerCase())) {
      toast('Equipamento já existe'); return;
    }
    const newList = [...(STATE.equipamentos || []), name];
    try {
      await setDoc(configDoc(cid, 'equipamentos'), { list: newList });
      newEqInput.value = '';
      toast('Equipamento adicionado');
      // STATE.equipamentos atualiza via onSnapshot, mas re-renderiza agora pra feedback imediato
    } catch (err) { toast('Erro: ' + err.message); }
  } }, '+ Adicionar');
  newEqInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); addEqBtn.click(); } });
  addEqRow.appendChild(newEqInput);
  addEqRow.appendChild(addEqBtn);
  panel.appendChild(addEqRow);
  // Re-render checkboxes quando equipamentos mudarem via snapshot
  eqBox.__rerender = renderEquipamentos;

  panel.appendChild(el('h2', {}, 'Sub-fichas (preparações)'));
  const sfWrap = el('div', {});
  function renderSubfichas() {
    sfWrap.innerHTML = '';
    dish.sub_fichas.forEach((sf, idx) => {
      const isFinal = idx === dish.sub_fichas.length - 1;
      const box = el('details', { class: 'subficha-editor', ...(isFinal ? { open: '' } : {}) });
      const summary = el('summary', { class: 'subficha-editor-summary' },
        el('span', { class: 'sf-editor-num' }, String(idx + 1).padStart(2, '0')),
        el('span', { class: 'sf-editor-name' }, sf.name || '(nova preparação)'),
        el('span', { class: 'sf-editor-meta' }, sf.rendimento || ''),
        el('span', { class: 'sf-editor-chev' }, '▾'),
        el('span', { class: 'sf-editor-actions', onclick: e => e.stopPropagation() },
          el('button', { class: 'btn btn-small', onclick: (e) => { e.preventDefault(); if (idx > 0) { [dish.sub_fichas[idx-1], dish.sub_fichas[idx]] = [dish.sub_fichas[idx], dish.sub_fichas[idx-1]]; renderSubfichas(); } }, title: 'Mover para cima' }, '↑'),
          el('button', { class: 'btn btn-small', onclick: (e) => { e.preventDefault(); if (idx < dish.sub_fichas.length - 1) { [dish.sub_fichas[idx], dish.sub_fichas[idx+1]] = [dish.sub_fichas[idx+1], dish.sub_fichas[idx]]; renderSubfichas(); } }, title: 'Mover para baixo' }, '↓'),
          el('button', { class: 'btn btn-small btn-danger', onclick: (e) => { e.preventDefault(); if (confirm('Excluir esta preparação?')) { dish.sub_fichas.splice(idx, 1); renderSubfichas(); } } }, '×')
        )
      );
      box.appendChild(summary);
      box.appendChild(el('div', { class: 'form-grid' },
          fieldSubfichaNameInput(dish, sf),
          fieldInput('Rendimento (quantidade)', 'number', sf.rendimento_qty ?? '', v => {
            const n = parseFloat(v);
            sf.rendimento_qty = isNaN(n) ? null : n;
            if (sf.rendimento_qty != null && sf.rendimento_unit) {
              sf.rendimento = `${String(sf.rendimento_qty).replace('.', ',')} ${sf.rendimento_unit}`;
            }
          }),
          fieldInput('Unidade (kg / l / und / porções)', 'text', sf.rendimento_unit || '', v => {
            sf.rendimento_unit = v.trim().toLowerCase();
            if (sf.rendimento_qty != null && sf.rendimento_unit) {
              sf.rendimento = `${String(sf.rendimento_qty).replace('.', ',')} ${sf.rendimento_unit}`;
            }
          })
        )
      );

      // Subproduto opcional — ex: "Joelho Cozido" rende joelho + caldo
      const hasSubproduto = !!(sf.subproduto && sf.subproduto.name);
      const subprodToggle = el('input', { type: 'checkbox' });
      if (hasSubproduto) subprodToggle.setAttribute('checked', '');
      const subprodFields = el('div', { class: 'subproduto-fields', style: hasSubproduto ? '' : 'display:none' });
      function renderSubprodFields() {
        subprodFields.innerHTML = '';
        if (!subprodToggle.checked) { subprodFields.style.display = 'none'; return; }
        subprodFields.style.display = '';
        if (!sf.subproduto) sf.subproduto = { id: 'bp-' + uid(), name: '', rendimento_qty: null, rendimento_unit: '' };
        if (!sf.subproduto.id) sf.subproduto.id = 'bp-' + uid();
        subprodFields.appendChild(el('p', { class: 'muted', style: 'font-size:0.82rem;margin-bottom:0.5rem;' },
          'O subproduto tem rendimento próprio e custo R$ 0,00 — o custo total fica no produto principal. Pode ser usado em outras sub-fichas e pratos.'));
        subprodFields.appendChild(el('div', { class: 'form-grid' },
          fieldInput('Nome do subproduto', 'text', sf.subproduto.name || '', v => sf.subproduto.name = v.trim()),
          fieldInput('Rendimento subproduto (qty)', 'number', sf.subproduto.rendimento_qty ?? '', v => {
            const n = parseFloat(v);
            sf.subproduto.rendimento_qty = isNaN(n) ? null : n;
          }),
          fieldInput('Unidade subproduto', 'text', sf.subproduto.rendimento_unit || '', v => {
            sf.subproduto.rendimento_unit = v.trim().toLowerCase();
          })
        ));
      }
      subprodToggle.addEventListener('change', () => {
        if (!subprodToggle.checked) {
          if (confirm('Remover o subproduto desta sub-ficha?')) {
            delete sf.subproduto;
            renderSubprodFields();
          } else {
            subprodToggle.checked = true;
          }
        } else {
          renderSubprodFields();
        }
      });
      renderSubprodFields();
      box.appendChild(el('label', { class: 'subprod-toggle-label' }, subprodToggle,
        el('span', {}, ' Esta sub-ficha também gera um subproduto (ex: caldo do joelho cozido)')));
      box.appendChild(subprodFields);

      box.appendChild(el('h4', {}, 'Insumos'));
      const ingList = el('div', {});
      function renderIngs() {
        ingList.innerHTML = '';
        sf.ingredientes.forEach((ing, ingIdx) => {
          const row = el('div', { class: 'ingredient-row' });
          const nameInput = el('input', { type: 'text', value: ing.insumo_name, placeholder: 'Insumo ou sub-ficha', list: 'all-insumos' });
          // Placeholder - reference set após varSelect ser criado
          const fcInput = el('input', { type: 'number', step: '0.01', min: '1', placeholder: 'FC', value: ing.fc || '', title: 'Fator de correção' });
          const varSelect = el('select', { class: 'var-select', title: 'Variação (ex: brunoise, julienne)' });
          const refBadge = el('span', { class: 'ref-badge', style: 'display:none' }, '');
          const unitInput = el('input', { type: 'text', value: ing.unit || '', placeholder: 'g' });
          function detectRef() {
            // Vínculo manual via subref_id sempre sobrepõe detecção automática
            if (ing.subref_id) {
              const manual = dish.sub_fichas.find(s => s.id === ing.subref_id && s.id !== sf.id);
              if (manual) return { type: 'subref', sf: manual };
            }
            // Verifica se o nome bate com uma sub-ficha do mesmo prato (exceto a atual)
            const n = nrm(nameInput.value);
            if (!n) return null;
            for (const otherSf of dish.sub_fichas) {
              if (otherSf.id === sf.id) continue;
              if (nrm(otherSf.name) === n) {
                return { type: 'subref', sf: otherSf };
              }
            }
            // Verifica subproduto (mesma ficha ou outra)
            const hit = findSubproduto(nameInput.value);
            if (hit && !(hit.dish.id === dish.id && hit.sf.id === sf.id)) {
              return { type: 'subproduto', hit };
            }
            return null;
          }
          function rebuildVarSelect() {
            const ref = detectRef();
            // Se for referência a sub-ficha ou subproduto, oculta FC/var e mostra badge
            if (ref) {
              fcInput.style.display = 'none';
              varSelect.style.display = 'none';
              refBadge.style.display = '';
              if (refBadge.__wrapEl) refBadge.__wrapEl.style.display = '';
              if (ref.type === 'subref') {
                refBadge.textContent = `↪ sub-ficha: ${ref.sf.name}`;
                refBadge.title = `Custo será puxado da sub-ficha "${ref.sf.name}"`;
                ing.subref_id = ref.sf.id;
                delete ing.fc;
                delete ing.variation_name;
                ing.insumo_id = '';
                // Auto-preenche unidade com o rendimento da sub-ficha referenciada
                const refRend = getSfRendimento(ref.sf);
                if (refRend.unit && !ing.unit) {
                  ing.unit = refRend.unit;
                  unitInput.value = refRend.unit;
                }
              } else {
                refBadge.textContent = `↳ subproduto de: ${ref.hit.dish.name}`;
                refBadge.title = `Subproduto da sub-ficha "${ref.hit.sf.name}" (${ref.hit.dish.name}) — custo zero`;
                delete ing.subref_id;
                delete ing.fc;
                delete ing.variation_name;
                ing.insumo_id = '';
                // Auto-preenche unidade com a unidade do subproduto
                const subU = ref.hit.subproduto.rendimento_unit || '';
                if (subU && !ing.unit) {
                  ing.unit = subU;
                  unitInput.value = subU;
                }
              }
              return;
            } else {
              refBadge.style.display = 'none';
              if (refBadge.__wrapEl) refBadge.__wrapEl.style.display = 'none';
              if (ing.subref_id) delete ing.subref_id;
            }
            const match = STATE.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
            const isReut = !!(match && match.reutilizavel);
            // Oculta FC e limpa se insumo é reutilizável
            if (isReut) {
              fcInput.style.display = 'none';
              fcInput.value = '';
              delete ing.fc;
            } else {
              fcInput.style.display = '';
            }
            varSelect.innerHTML = '';
            const vars = (match && Array.isArray(match.variations)) ? match.variations : [];
            if (vars.length === 0 || isReut) {
              varSelect.style.display = 'none';
              return;
            }
            varSelect.style.display = '';
            varSelect.appendChild(el('option', { value: '' }, '— sem variação —'));
            vars.forEach(v => {
              const opt = el('option', { value: v.name }, `${v.name} (FC ${String(v.fc).replace('.', ',')})`);
              if (ing.variation_name && ing.variation_name.toLowerCase() === v.name.toLowerCase()) {
                opt.setAttribute('selected', '');
              }
              varSelect.appendChild(opt);
            });
          }
          nameInput.addEventListener('input', () => {
            ing.insumo_name = nameInput.value;
            const ref = detectRef();
            if (!ref) {
              const match = STATE.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
              ing.insumo_id = match ? match.id : slugify(nameInput.value);
            }
            rebuildVarSelect();
          });
          varSelect.addEventListener('change', () => {
            const match = STATE.insumos.find(i => i.id === ing.insumo_id);
            const chosen = (match && match.variations || []).find(v => v.name === varSelect.value);
            if (chosen) {
              ing.variation_name = chosen.name;
              ing.fc = chosen.fc;
              fcInput.value = chosen.fc;
            } else {
              delete ing.variation_name;
              delete ing.fc;
              fcInput.value = '';
            }
          });
          rebuildVarSelect();
          row.appendChild(nameInput);
          // Dropdown manual de vínculo a sub-ficha (override do detectRef por nome)
          const subrefSelect = el('select', { class: 'subref-select', title: 'Vincular esta linha a uma sub-ficha do prato (sobrepõe detecção por nome)' });
          function rebuildSubrefSelect() {
            subrefSelect.innerHTML = '';
            const optNone = el('option', { value: '' }, '↪ sub-ficha? (auto)');
            subrefSelect.appendChild(optNone);
            (dish.sub_fichas || []).forEach((otherSf, oIdx) => {
              if (otherSf.id === sf.id) return;
              const opt = el('option', { value: otherSf.id }, `↪ ${otherSf.name || `sub-ficha ${oIdx+1}`}`);
              if (ing.subref_id === otherSf.id) opt.setAttribute('selected', '');
              subrefSelect.appendChild(opt);
            });
          }
          rebuildSubrefSelect();
          subrefSelect.addEventListener('change', () => {
            if (subrefSelect.value) {
              ing.subref_id = subrefSelect.value;
              delete ing.fc;
              delete ing.variation_name;
              ing.insumo_id = '';
              const ref = (dish.sub_fichas || []).find(s => s.id === subrefSelect.value);
              if (ref) {
                const refRend = getSfRendimento(ref);
                if (refRend.unit && !ing.unit) {
                  ing.unit = refRend.unit;
                  unitInput.value = refRend.unit;
                }
              }
            } else {
              delete ing.subref_id;
            }
            rebuildVarSelect();
          });
          row.appendChild(subrefSelect);
          row.appendChild(varSelect);
          const qtyInput = el('input', { type: 'number', step: '0.01', value: ing.qty != null ? ing.qty : '', placeholder: 'Qtd' });
          qtyInput.addEventListener('input', () => {
            const v = parseFloat(qtyInput.value);
            ing.qty = isNaN(v) ? null : v;
            ing.qty_raw = qtyInput.value;
            ing.is_qb = qtyInput.value.toLowerCase() === 'q.b';
          });
          row.appendChild(qtyInput);
          unitInput.addEventListener('input', () => { ing.unit = unitInput.value; });
          row.appendChild(unitInput);
          const obsInput = el('input', { type: 'text', value: ing.observacao || '', placeholder: 'Observação' });
          obsInput.addEventListener('input', () => { ing.observacao = obsInput.value; });
          row.appendChild(obsInput);
          fcInput.addEventListener('input', () => {
            const v = parseFloat(fcInput.value);
            if (!v || v <= 1) delete ing.fc;
            else ing.fc = v;
          });
          row.appendChild(fcInput);
          row.appendChild(el('button', { class: 'btn btn-small btn-danger', onclick: () => { sf.ingredientes.splice(ingIdx, 1); renderIngs(); } }, '×'));
          ingList.appendChild(row);
          // Badge de referência (sub-ficha ou subproduto) - sempre inserido, visibilidade controlada por detectRef
          const badgeWrap = el('div', { class: 'ref-badge-wrap' }, refBadge);
          badgeWrap.style.display = (refBadge.style.display === 'none') ? 'none' : '';
          ingList.appendChild(badgeWrap);
          // Atualiza wrap visibility quando rebuildVarSelect for chamado
          refBadge.__wrapEl = badgeWrap;
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
    const sfActions = el('div', { class: 'sf-add-actions' });
    sfActions.appendChild(el('button', { class: 'btn', onclick: () => {
      dish.sub_fichas.push({ id: uid(), name: `Preparação ${dish.sub_fichas.length + 1}`, rendimento: '', ingredientes: [], modo_preparo: '' });
      renderSubfichas();
    } }, '+ Adicionar sub-ficha'));
    sfActions.appendChild(el('button', { class: 'btn btn-accent', onclick: () => {
      openCopySubfichaModal(dish, (copied) => {
        dish.sub_fichas.push(copied);
        renderSubfichas();
        toast('Sub-ficha copiada');
      });
    } }, '↗ Copiar de outra ficha'));
    sfWrap.appendChild(sfActions);
  }
  renderSubfichas();
  panel.appendChild(sfWrap);

  const dl = el('datalist', { id: 'all-insumos' });
  STATE.insumos.forEach(i => dl.appendChild(el('option', { value: i.name })));
  // Sub-fichas do prato atual (pra usar uma sub-ficha anterior como ingrediente)
  (dish.sub_fichas || []).forEach(otherSf => {
    if (otherSf.name) dl.appendChild(el('option', { value: otherSf.name }, 'sub-ficha deste prato'));
  });
  // Adiciona subprodutos de todos os pratos como opções (identificadas como "subproduto")
  listAllSubprodutos().forEach(({ dish: d, subproduto }) => {
    dl.appendChild(el('option', { value: subproduto.name }, `subproduto de ${d.name}`));
  });
  panel.appendChild(dl);

  panel.appendChild(el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn btn-primary', onclick: () => saveDishAction(cid, dish, dishId) }, 'Salvar'),
    el('a', { class: 'btn', href: `#/c/${cid}/admin` }, 'Cancelar')
  ));
  app.appendChild(panel);
  // Sticky save bar sempre visível durante a edição
  const stickyBar = el('div', { class: 'sticky-save-bar' },
    el('span', { class: 'muted', style: 'font-size:0.82rem;' }, 'Editando ficha — salve antes de sair'),
    el('div', { style: 'display:flex;gap:0.5rem;' },
      el('a', { class: 'btn btn-small', href: `#/c/${cid}/admin` }, 'Cancelar'),
      el('button', { class: 'btn btn-small btn-primary', onclick: () => saveDishAction(cid, dish, dishId) }, 'Salvar')
    )
  );
  app.appendChild(stickyBar);
}

async function saveDishAction(cid, dish, originalId) {
  if (!dish.name.trim()) { alert('Nome do prato é obrigatório'); return; }
  const newId = slugify(dish.name);
  dish.id = newId;
  // Cliente não pode alterar lock — força ao valor original
  if (!canToggleDishLock(cid)) {
    const orig = originalId ? STATE.dishes.find(d => d.id === originalId) : null;
    dish.locked = orig ? !!orig.locked : false; // nova ficha do cliente nasce desbloqueada
  }
  try {
    // Coleta nomes de subprodutos (global + da ficha atual) pra evitar criar insumo pra eles
    const subprodutoNames = new Set();
    for (const sf of dish.sub_fichas) {
      if (sf.subproduto && sf.subproduto.name) subprodutoNames.add(nrm(sf.subproduto.name));
    }
    STATE.dishes.forEach(d => {
      if (d.id === dish.id) return;
      for (const sf of d.sub_fichas || []) {
        if (sf.subproduto && sf.subproduto.name) subprodutoNames.add(nrm(sf.subproduto.name));
      }
    });
    // Nomes de sub-fichas do próprio prato (subref por detecção) também não devem virar insumo
    const sfNames = new Set((dish.sub_fichas || []).map(s => nrm(s.name)).filter(Boolean));

    // Register new insumos
    const batch = writeBatch(db);
    let bc = 0;
    for (const sf of dish.sub_fichas) {
      for (const ing of sf.ingredientes) {
        if (!ing.insumo_name.trim()) continue;
        const ingNrm = nrm(ing.insumo_name);
        // Skip se for subproduto ou nome de sub-ficha (subref) — esses não são insumos
        if (subprodutoNames.has(ingNrm) || sfNames.has(ingNrm) || ing.subref_id) {
          ing.insumo_id = '';
          continue;
        }
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
    // Remove insumos órfãos (que deixaram de ser usados)
    cleanupOrphanInsumos(cid).catch(err => console.warn('cleanup:', err));
    toast('Salvo');
    location.hash = `#/c/${cid}/ficha/${dish.id}`;
  } catch (err) { console.error(err); toast('Erro: ' + err.message); }
}

// Input específico para nome de sub-ficha — alerta quando rename vai quebrar vínculos por nome
function fieldSubfichaNameInput(dish, sf, onAfterMigrate) {
  const wrap = el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome da preparação'));
  const input = el('input', { type: 'text', value: sf.name ?? '' });
  const hint = el('div', { class: 'rename-hint', style: 'display:none;' });
  let originalName = sf.name || '';
  function checkRefs() {
    const newName = sf.name || '';
    if (nrm(newName) === nrm(originalName)) { hint.style.display = 'none'; return; }
    const sfIdx = dish.sub_fichas.findIndex(s => s.id === sf.id);
    if (sfIdx < 0) { hint.style.display = 'none'; return; }
    const matches = [];
    const oldClean = nrm(originalName);
    if (oldClean.length < 4) { hint.style.display = 'none'; return; }
    for (let j = sfIdx + 1; j < dish.sub_fichas.length; j++) {
      const consumer = dish.sub_fichas[j];
      for (const ing of consumer.ingredientes || []) {
        if (ing.subref_id) continue;
        const ingClean = nrm(ing.insumo_name);
        if (ingClean.length < 4) continue;
        if (oldClean === ingClean) { matches.push({ consumer, ing }); continue; }
        if (oldClean.includes(ingClean) || ingClean.includes(oldClean)) {
          const ratio = Math.min(oldClean.length, ingClean.length) / Math.max(oldClean.length, ingClean.length);
          if (ratio > 0.55) matches.push({ consumer, ing });
        }
      }
    }
    if (matches.length === 0) { hint.style.display = 'none'; return; }
    hint.innerHTML = '';
    hint.style.display = '';
    hint.appendChild(el('span', { class: 'rename-hint-text' },
      `⚠ ${matches.length} ingrediente(s) em outras preparações vinculam aqui pelo nome antigo "${originalName}". Renomear sem migrar vai quebrar o vínculo.`
    ));
    const btn = el('button', { class: 'btn btn-small btn-accent', onclick: (e) => {
      e.preventDefault();
      matches.forEach(({ ing }) => { ing.subref_id = sf.id; });
      toast(`${matches.length} vínculo(s) convertido(s) para manual`);
      hint.style.display = 'none';
      originalName = sf.name;
      if (typeof onAfterMigrate === 'function') onAfterMigrate();
    } }, `Vincular manualmente (${matches.length})`);
    hint.appendChild(btn);
  }
  input.addEventListener('input', () => { sf.name = input.value; });
  input.addEventListener('blur', checkRefs);
  wrap.appendChild(input);
  wrap.appendChild(hint);
  return wrap;
}

function fieldInput(label, type, value, onChange) {
  const wrap = el('label', { class: 'field' }, el('span', { class: 'label-text' }, label));
  const input = el('input', { type, value: value ?? '' });
  input.addEventListener('input', () => onChange(input.value));
  wrap.appendChild(input);
  return wrap;
}

// ---------- Exports ----------
// Agrega todos os insumos necessários para produzir o prato (ignorando subrefs — eles são contados via suas sub-fichas)
function buildShoppingList(dish, scales) {
  const agg = {};
  for (const sf of dish.sub_fichas) {
    const scale = scales[sf.id] || 1;
    for (const ing of (sf.ingredientes || [])) {
      if (ing.subref_id) continue;
      if (ing.qty == null) continue;
      // Subproduto (produzido em outra sub-ficha) — não entra na lista de compras
      const subprodHit = findSubproduto(ing.insumo_name);
      if (subprodHit && !(subprodHit.dish.id === dish.id && subprodHit.sf.id === sf.id)) continue;
      const insumo = findInsumo(ing.insumo_id);
      if (!insumo) continue;
      const [qn, pn] = normalizeQtyPrice(ing, insumo);
      if (qn == null || pn == null) continue;
      // Insumos reutilizáveis: aparecem na lista mas com custo zero (lançar em despesas operacionais)
      const isReutilizavel = !!insumo.reutilizavel;
      const fc = isReutilizavel ? 1 : (ing.fc || 1);
      const key = ing.insumo_id;
      if (!agg[key]) agg[key] = { name: insumo.name, unit: insumo.unit, qty: 0, cost: 0, isReutilizavel };
      agg[key].qty += qn * scale * fc;
      agg[key].cost += isReutilizavel ? 0 : (qn * pn * scale * fc);
    }
  }
  return Object.values(agg).sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
}

function exportFichaPDF(dish, state) {
  const { jsPDF } = window.jspdf;
  const docPdf = new jsPDF({ unit: 'mm', format: 'a4' });
  const margin = 15;
  let y = margin;
  const pageWidth = docPdf.internal.pageSize.getWidth();
  const contentWidth = pageWidth - margin * 2;
  const isTrabalho = state.view === 'trabalho';
  const all = dishCost(dish);
  const scales = typeof state.scales === 'function' ? state.scales() : {};
  const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
  const finalScale = scales[finalSf?.id] || 1;
  const scaleChanged = finalScale !== 1;
  const currentSf = (dish.sub_fichas || []).find(s => s.id === state.activeSf) || (dish.sub_fichas || [])[0];
  const sfScale = scales[currentSf?.id] || 1;

  // --- Cabeçalho ---
  docPdf.setFont('times', 'italic').setFontSize(10).setTextColor(130);
  docPdf.text(isTrabalho ? 'Ficha Técnica Operacional' : 'Ficha de Custo por Rendimento', pageWidth / 2, y, { align: 'center' });
  y += 7;
  docPdf.setFont('times', 'normal').setFontSize(22).setTextColor(20);
  docPdf.text(dish.name, pageWidth / 2, y, { align: 'center' });
  y += 10;
  docPdf.setDrawColor(184, 149, 94).setLineWidth(0.3);
  docPdf.line(pageWidth / 2 - 20, y, pageWidth / 2 + 20, y);
  y += 8;

  const headStyle = { fillColor: [247, 245, 238], textColor: [80, 80, 80], fontStyle: 'bold', fontSize: 8, cellPadding: 2.5 };
  const bodyStyle = { fontSize: 9, cellPadding: 2.5, textColor: [40, 40, 40] };
  const altRowStyle = { fillColor: [252, 251, 247] };

  if (isTrabalho) {
    // --- Meta: rendimento alvo ---
    const finalR = getSfRendimento(finalSf);
    const targetQty = state.finalTargetQty || finalR.qty;
    docPdf.setFont('helvetica', 'normal').setFontSize(10).setTextColor(80);
    docPdf.text(`Produção: ${fmtNum(targetQty, targetQty % 1 === 0 ? 0 : 2)} ${finalR.unit}`, pageWidth / 2, y, { align: 'center' });
    if (scaleChanged) {
      y += 5;
      docPdf.setFontSize(8).setTextColor(120);
      docPdf.text(`(escalado ${fmtNum(finalScale, 2)}× do original)`, pageWidth / 2, y, { align: 'center' });
    }
    y += 10;

    // --- 1. Lista de compras (só quantidades, sem preços — é PDF de produção) ---
    const shopping = buildShoppingList(dish, scales);
    if (shopping.length > 0) {
      docPdf.setFont('times', 'normal').setFontSize(14).setTextColor(20);
      docPdf.text('Lista de compras (total necessário)', margin, y); y += 5;
      docPdf.autoTable({
        startY: y, margin: { left: margin, right: margin },
        head: [['Insumo', 'Quantidade total']],
        body: shopping.map(s => {
          const norm = normUnitForDisplay(s.qty, s.unit);
          const label = s.isReutilizavel ? `${s.name} (rateio)` : s.name;
          return [label, `${norm.text} ${norm.unit}`.trim()];
        }),
        theme: 'plain',
        headStyles: headStyle, bodyStyles: bodyStyle, alternateRowStyles: altRowStyle,
        columnStyles: { 1: { halign: 'right' } }
      });
      y = docPdf.lastAutoTable.finalY + 10;
    }

    // --- 2. Cada sub-ficha em sequência (ordem invertida: final primeiro) ---
    [...dish.sub_fichas].reverse().forEach((s) => {
      const idx = dish.sub_fichas.indexOf(s);
      if (y > 230) { docPdf.addPage(); y = margin; }
      const scale = scales[s.id] || 1;
      // Título da sub-ficha
      docPdf.setFont('times', 'normal').setFontSize(13).setTextColor(20);
      docPdf.text(`${idx + 1}. ${s.name}`, margin, y); y += 5;
      docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(120);
      docPdf.text(`Rendimento: ${sfRendimentoText(s, scale)}${scale !== 1 ? ` (${fmtNum(scale, 2)}× original)` : ''}`, margin, y);
      y += 6;
      // Tabela de ingredientes
      docPdf.autoTable({
        startY: y, margin: { left: margin, right: margin },
        head: [['Insumo', 'Quantidade', 'Observação']],
        body: (s.ingredientes || []).map(i => {
          const ref = i.subref_id ? dish.sub_fichas.find(x => x.id === i.subref_id) : null;
          const f = formatIngQty(i, scale);
          return [
            (ref ? '↪ ' : '') + i.insumo_name,
            `${f.text} ${f.unit}`.trim(),
            i.observacao || '—'
          ];
        }),
        theme: 'plain',
        headStyles: headStyle, bodyStyles: bodyStyle, alternateRowStyles: altRowStyle
      });
      y = docPdf.lastAutoTable.finalY + 5;
      // Modo de preparo
      if (s.modo_preparo) {
        if (y > 250) { docPdf.addPage(); y = margin; }
        docPdf.setFont('helvetica', 'bold').setFontSize(9).setTextColor(100);
        docPdf.text('MODO DE PREPARO', margin, y); y += 4;
        docPdf.setFont('helvetica', 'normal').setFontSize(9).setTextColor(40);
        const lines = docPdf.splitTextToSize(s.modo_preparo.replace(/\s+/g, ' '), contentWidth);
        docPdf.text(lines, margin, y);
        y += lines.length * 4 + 8;
      } else {
        y += 4;
      }
    });

    // --- 3. Louça + equipamentos ---
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
      docPdf.text('EQUIPAMENTOS NECESSÁRIOS', margin, y); y += 5;
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
  // Rodapé em TODAS as páginas: nome do cliente + consultoria (se ativa)
  const totalPages = docPdf.internal.getNumberOfPages();
  const cliente = STATE.currentCliente;
  const footerParts = [];
  if (cliente?.name) footerParts.push(cliente.name);
  if (cliente?.show_consultor !== false && cliente?.consultor_name) {
    let c = 'Consultoria por ' + cliente.consultor_name;
    if (cliente.consultor_info) c += ' · ' + cliente.consultor_info;
    footerParts.push(c);
  }
  const footerText = footerParts.join('  ·  ');
  if (footerText) {
    for (let p = 1; p <= totalPages; p++) {
      docPdf.setPage(p);
      docPdf.setFont('helvetica', 'italic').setFontSize(7).setTextColor(140);
      docPdf.text(footerText, pageWidth / 2, docPdf.internal.pageSize.getHeight() - 8, { align: 'center' });
      docPdf.setFont('helvetica', 'normal').setFontSize(7).setTextColor(170);
      docPdf.text(`${p} / ${totalPages}`, pageWidth - margin, docPdf.internal.pageSize.getHeight() - 8, { align: 'right' });
    }
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
    ['Equipamentos', (dish.equipamentos || []).join('; ')],
    [],
    ['Restaurante', STATE.currentCliente?.name || '—'],
    ...((STATE.currentCliente?.show_consultor !== false && STATE.currentCliente?.consultor_name) ? [
      ['Consultoria', STATE.currentCliente.consultor_name + (STATE.currentCliente.consultor_info ? ' · ' + STATE.currentCliente.consultor_info : '')]
    ] : [])
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
  // Lista de compras (sempre adicionada na aba "Lista de Compras")
  const shopping = buildShoppingList(dish, scales);
  if (shopping.length > 0) {
    const shoppingData = [
      [`LISTA DE COMPRAS${scaleChanged ? ` — produção escalada ${fmtNum(finalScale, 2)}×` : ''}`],
      [`Rendimento alvo`, `${fmtNum(state?.finalTargetQty || 0, 2)} ${state?.finalUnit || ''}`],
      [],
      ['Insumo', 'Quantidade', 'Unidade', 'Custo'],
      ...shopping.map(s => {
        const norm = normUnitForDisplay(s.qty, s.unit);
        const label = s.isReutilizavel ? `${s.name} (rateio — despesas)` : s.name;
        return [label, norm.text, norm.unit, s.cost];
      }),
      [],
      ['TOTAL', '', '', shopping.reduce((t, s) => t + s.cost, 0)]
    ];
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(shoppingData), 'Lista de Compras');
  }
  // Sequência completa de sub-fichas escaladas
  if (scaleChanged && state?.view === 'trabalho') {
    const scaledAll = [[`PRODUÇÃO ESCALADA — alvo final ${fmtNum(state.finalTargetQty, 2)} ${state.finalUnit}`], []];
    [...dish.sub_fichas].reverse().forEach((s) => {
      const idx = dish.sub_fichas.indexOf(s);
      const sScale = scales[s.id] || 1;
      scaledAll.push([`${idx+1}. ${s.name}${sScale !== 1 ? ` (× ${fmtNum(sScale, 2)})` : ''}`]);
      scaledAll.push(['Rendimento', sfRendimentoText(s, sScale)]);
      scaledAll.push([]);
      scaledAll.push(['Insumo', 'Quantidade', 'Unidade', 'Observação']);
      (s.ingredientes || []).forEach(i => {
        const f = formatIngQty(i, sScale);
        scaledAll.push([i.insumo_name, f.text || '', f.unit || '', i.observacao || '']);
      });
      if (s.modo_preparo) {
        scaledAll.push([]);
        scaledAll.push(['Modo de preparo']);
        scaledAll.push([s.modo_preparo]);
      }
      scaledAll.push([]);
    });
    XLSX.utils.book_append_sheet(wb, XLSX.utils.aoa_to_sheet(scaledAll), 'Receita Completa');
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

// Backup do cliente agora é feito via Gerenciar Restaurantes (botão ↓ Backup por cliente)
