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
  // Auto-cleanup: remove insumos órfãos (que ficaram sem nenhuma ficha referenciando)
  await cleanupOrphanInsumos(cid).catch(err => console.warn('cleanup insumos:', err));
}

// Remove insumos que não são referenciados por nenhuma ficha
async function cleanupOrphanInsumos(cid) {
  const [insumosSnap, dishesSnap] = await Promise.all([
    getDocs(insumosCol(cid)),
    getDocs(dishesCol(cid))
  ]);
  // IDs de insumos referenciados por qualquer ingrediente real (não subref)
  const referenced = new Set();
  dishesSnap.docs.forEach(d => {
    const data = d.data();
    (data.sub_fichas || []).forEach(sf => {
      (sf.ingredientes || []).forEach(ing => {
        if (!ing.subref_id && ing.insumo_id) referenced.add(ing.insumo_id);
      });
    });
  });
  // Deletar insumos não referenciados
  let batch = writeBatch(db);
  let count = 0, deleted = 0;
  for (const ins of insumosSnap.docs) {
    if (!referenced.has(ins.id)) {
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
      el('div', { class: 'modal-actions' },
        el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
        el('button', { class: 'btn btn-primary', onclick: async () => {
          const updated = {
            ...cliente,
            name: nameInput.value.trim() || cliente.name,
            consultor_name: consultorNameInput.value.trim(),
            consultor_info: consultorInfoInput.value.trim(),
            show_consultor: showToggle.checked
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
    ['nome', 'unidade', 'preco'],
    ['Cebola Branca', 'kg', 4.50],
    ['Sal Refinado', 'kg', 3.20],
    ['Azeite', 'l', 28.00],
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
    insumosByName[name.toLowerCase()] = { id, name, unit, price };
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
  return getSfRendimento(sf);
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
  const finalRend = getSfRendimento(finalSf);
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
    const costInfo = dishCost(dish);
    const { costPerPortion, suggestedPrice, cmv, markup } = costInfo;
    const lastSf = (dish.sub_fichas || [])[dish.sub_fichas.length - 1];
    const rendDisplay = lastSf?.rendimento ? formatRendimento(lastSf.rendimento) : '—';
    const finalSfId = lastSf?.id;
    const showCost = canEditInsumoPrice(cid);
    const dishGroup = el('article', { class: 'dish-group' },
      el('header', { class: 'dish-head' },
        el('a', { class: 'dish-title', href: `#/c/${cid}/ficha/${dish.id}` },
          el('span', { class: 'dish-number' }, String(idx + 1).padStart(2, '0')),
          el('span', { class: 'dish-name' }, dish.name)
        ),
        el('div', { class: 'dish-meta' },
          el('span', { class: 'meta-item' }, el('em', {}, 'rendimento '), rendDisplay),
          showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'custo/porção '), fmtBRL(costPerPortion)) : null,
          showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'preço venda '), fmtBRL(suggestedPrice)) : null,
          showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'CMV '), fmtNum(cmv, 1) + '%') : null,
          showCost ? el('span', { class: 'meta-item' }, el('em', {}, 'markup '), fmtNum(markup, 0) + '%') : null
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
  const wrap = el('div', { class: 'cliente-nav' });
  // Back link (só para master/staff) em linha superior separada
  if (isMaster() || isStaff()) {
    wrap.appendChild(el('a', { href: '#/clientes', class: 'cliente-back' }, '← Todos os restaurantes'));
  }
  // Tabs com active state baseado na rota atual
  const hash = location.hash.slice(1) || '/';
  const isAdmin = hash.includes(`/c/${cid}/admin`);
  const isInsumos = hash.includes(`/c/${cid}/insumos`);
  const isCardapio = !isAdmin && !isInsumos;
  const tabs = el('nav', { class: 'cliente-tabs' },
    el('a', { class: 'cliente-tab' + (isCardapio ? ' active' : ''), href: `#/c/${cid}` },
      el('span', { class: 'tab-icon' }, '◉'), 'Cardápio'),
    el('a', { class: 'cliente-tab' + (isInsumos ? ' active' : ''), href: `#/c/${cid}/insumos` },
      el('span', { class: 'tab-icon' }, '◎'), 'Insumos'),
    canEditCliente(cid) ? el('a', { class: 'cliente-tab' + (isAdmin ? ' active' : ''), href: `#/c/${cid}/admin` },
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

  // Top action bar — view toggle + exports (sticky)
  const toggle = el('div', { class: 'view-toggle' },
    el('button', { 'data-view': 'trabalho' }, 'Trabalho'),
    el('button', { 'data-view': 'custo' }, 'Custo')
  );
  const exports = el('div', { class: 'export-actions' },
    el('button', { class: 'btn btn-small', title: 'Exportar PDF', onclick: () => exportFichaPDF(dish, state) }, '📄 PDF'),
    el('button', { class: 'btn btn-small', title: 'Exportar Excel', onclick: () => exportFichaXLSX(dish, state) }, '📊 Excel')
  );
  const actionBar = el('div', { class: 'ficha-action-bar' }, toggle, exports);
  app.appendChild(actionBar);

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
  const quickNav = el('nav', { class: 'sf-quicknav' });
  const subFichasReversed = [...(dish.sub_fichas || [])].reverse();
  subFichasReversed.forEach((sf) => {
    const originalIdx = dish.sub_fichas.indexOf(sf);
    const isFinal = originalIdx === dish.sub_fichas.length - 1;
    quickNav.appendChild(el('a', { href: `#sf-${sf.id}`, class: isFinal ? 'is-final' : '' },
      `${originalIdx + 1}. ${sf.name}`
    ));
  });
  app.appendChild(quickNav);

  // Body (all sub-fichas stacked — ordem invertida no modo trabalho)
  const body = el('div', { id: 'ficha-body-container' });
  app.appendChild(body);

  function updateBody() {
    $$('.view-toggle button', toggle).forEach(b => b.classList.toggle('active', b.dataset.view === state.view));
    scaleBar.style.display = state.view === 'trabalho' ? '' : 'none';
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

  // Ingredientes
  const sfIdx = idx;
  const ingList = el('ul', { class: 'kitchen-ingredients' });
  (sf.ingredientes || []).forEach(ing => {
    let subSf = null;
    if (ing.subref_id) subSf = dish.sub_fichas.find(s => s.id === ing.subref_id);
    if (!subSf && sfIdx > 0) subSf = detectSubref(dish, sfIdx, ing.insumo_name);

    const formatted = formatIngQty(ing, sfScale);
    const displayName = (subSf ? '↪ ' : '') + ing.insumo_name + (ing.variation_name ? ` — ${ing.variation_name}` : '');
    const li = el('li', { class: 'kitchen-ing' + (subSf ? ' is-subref' : '') },
      el('div', { class: 'k-row-main' },
        el('span', { class: 'k-name' }, displayName),
        el('span', { class: 'k-qty' },
          el('span', { class: 'qty-num' }, formatted.text),
          formatted.unit ? el('span', { class: 'qty-unit' }, formatted.unit) : null
        )
      ),
      ing.observacao ? el('div', { class: 'k-obs' }, ing.observacao) : null,
      subSf ? el('div', { class: 'k-sub-note' }, el('a', { href: `#sf-${subSf.id}` }, 'ver preparação: ' + subSf.name)) : null
    );
    ingList.appendChild(li);
  });
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
    const tbl = el('table', { class: 'ficha-table' },
      el('thead', {}, el('tr', {},
        el('th', {}, 'Insumo'),
        el('th', { class: 'num' }, 'Qtd'),
        el('th', {}, 'Un.'),
        el('th', { class: 'num' }, 'Preço unit.'),
        el('th', { class: 'num' }, 'Custo')
      )),
      el('tbody', {}, ...rows.map(({ ing, insumo, cost, isSubref, subSf, fc }) => {
        const fmt = formatIngQty(ing);
        let priceTxt, nameCell;
        if (isSubref && subSf) {
          priceTxt = el('span', { class: 'subref-note' }, `sub-ficha`);
          nameCell = el('td', { 'data-label': 'Insumo', class: 'subref-cell' },
            el('span', { class: 'subref-arrow' }, '↪ '), ing.insumo_name);
        } else {
          const normPrice = insumo ? normalizePriceForDisplay(insumo) : null;
          priceTxt = normPrice ? `${fmtBRL(normPrice.price)} / ${normPrice.unit}` : '—';
          const fcBadge = (fc && fc > 1) ? el('span', { class: 'fc-badge', title: `Fator de correção ${fc}x — considera perda no processamento` }, ` · FC ${fmtNum(fc, 2)}`) : null;
          const varBadge = ing.variation_name ? el('span', { class: 'var-badge', title: 'Variação do insumo' }, ` — ${ing.variation_name}`) : null;
          nameCell = el('td', { 'data-label': 'Insumo' }, ing.insumo_name, varBadge, fcBadge);
        }
        return el('tr', { class: isSubref ? 'row-subref' : '' },
          nameCell,
          el('td', { class: 'num', 'data-label': 'Qtd' }, fmt.text),
          el('td', { 'data-label': 'Un.' }, fmt.unit || '—'),
          el('td', { class: 'num', 'data-label': 'Preço unit.' }, priceTxt),
          el('td', { class: 'num', 'data-label': 'Custo' }, fmtBRL(cost))
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
      el('th', {}, 'Variações'),
      el('th', {}, 'Usado em')
    ))
  );
  const tbody = el('tbody', {});
  const usageMap = {};
  STATE.dishes.forEach(d => (d.sub_fichas || []).forEach(sf =>
    (sf.ingredientes || []).forEach(i => { usageMap[i.insumo_id] = (usageMap[i.insumo_id] || 0) + 1; })
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
        const cells = [
          el('td', { 'data-label': 'Insumo' }, insumo.name),
          el('td', { 'data-label': 'Unidade' }, unitInput),
          el('td', { 'data-label': 'Preço (R$)' }, priceInput),
          el('td', { 'data-label': 'Variações' }, varBtn),
          el('td', { 'data-label': 'Usado em' }, (usageMap[insumo.id] || 0) + ' receitas'),
        ];
        tbody.appendChild(el('tr', {}, ...cells));
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

// Modal de variações do insumo (ex: Cebola branca → brunoise FC 1,15, julienne FC 1,12)
function openVariationsModal(cid, insumo) {
  const variations = JSON.parse(JSON.stringify(insumo.variations || []));
  const modal = el('div', { class: 'modal' },
    el('div', { class: 'modal-overlay', onclick: () => modal.remove() })
  );
  const content = el('div', { class: 'modal-content modal-wide' },
    el('h2', {}, 'Variações de ' + insumo.name),
    el('p', { class: 'modal-subtitle' },
      'Variações são cortes ou preparos do insumo base (ex: brunoise, julienne) com fator de correção próprio. ' +
      'Nas fichas, selecionar uma variação carrega o FC automaticamente. Na lista de compras, agrega no insumo base.')
  );
  const varList = el('div', { class: 'variations-list' });
  function renderVars() {
    varList.innerHTML = '';
    if (variations.length === 0) {
      varList.appendChild(el('p', { class: 'muted empty-variations' }, 'Nenhuma variação cadastrada. Clique em "+ Adicionar variação" abaixo.'));
    }
    variations.forEach((v, idx) => {
      const nameI = el('input', { type: 'text', value: v.name || '', placeholder: 'Ex: brunoise' });
      nameI.addEventListener('input', () => v.name = nameI.value);
      const fcI = el('input', { type: 'text', inputmode: 'decimal', value: v.fc != null ? String(v.fc).replace('.', ',') : '', placeholder: 'Ex: 1,15' });
      fcI.addEventListener('input', () => {
        const n = parseFloat(fcI.value.replace(',', '.'));
        v.fc = isNaN(n) ? null : n;
      });
      const delBtn = el('button', { class: 'btn btn-small btn-danger', onclick: () => {
        variations.splice(idx, 1); renderVars();
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
  } }, '+ Adicionar variação'));
  content.appendChild(el('div', { class: 'modal-actions' },
    el('button', { class: 'btn', onclick: () => modal.remove() }, 'Cancelar'),
    el('button', { class: 'btn btn-primary', onclick: async () => {
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
        insumo.variations = clean.length > 0 ? clean : undefined;
        if (insumo.variations === undefined) delete insumo.variations;
        await saveInsumo(cid, insumo);
        toast('Variações salvas');
        modal.remove();
      } catch (err) { toast('Erro: ' + err.message); }
    } }, 'Salvar')
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
        const formEl = el('div', { class: 'form-grid' },
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Nome'), nameInput),
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Unidade'), unitInput),
          el('label', { class: 'field' }, el('span', { class: 'label-text' }, 'Preço (R$)'), priceInput)
        );
        modal.__submit = async () => {
          const name = nameInput.value.trim();
          const unit = unitInput.value || 'kg';
          const price = parseFloat(priceInput.value) || 0;
          if (!name) { alert('Nome obrigatório'); return; }
          const id = slugify(name);
          if (STATE.insumos.find(i => i.id === id)) { alert('Já existe um insumo com esse nome'); return; }
          try {
            await saveInsumo(cid, { id, name, unit, price });
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
          fieldInput('Nome da preparação', 'text', sf.name, v => sf.name = v),
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
      box.appendChild(el('h4', {}, 'Insumos'));
      const ingList = el('div', {});
      function renderIngs() {
        ingList.innerHTML = '';
        sf.ingredientes.forEach((ing, ingIdx) => {
          const row = el('div', { class: 'ingredient-row' });
          const nameInput = el('input', { type: 'text', value: ing.insumo_name, placeholder: 'Insumo', list: 'all-insumos' });
          // Placeholder - reference set após varSelect ser criado
          const fcInput = el('input', { type: 'number', step: '0.01', min: '1', placeholder: 'FC', value: ing.fc || '', title: 'Fator de correção' });
          const varSelect = el('select', { class: 'var-select', title: 'Variação (ex: brunoise, julienne)' });
          function rebuildVarSelect() {
            const match = STATE.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
            varSelect.innerHTML = '';
            const vars = (match && Array.isArray(match.variations)) ? match.variations : [];
            if (vars.length === 0) {
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
            const match = STATE.insumos.find(i => i.name.toLowerCase() === nameInput.value.toLowerCase());
            ing.insumo_id = match ? match.id : slugify(nameInput.value);
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
          row.appendChild(varSelect);
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
    // Remove insumos órfãos (que deixaram de ser usados)
    cleanupOrphanInsumos(cid).catch(err => console.warn('cleanup:', err));
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
// Agrega todos os insumos necessários para produzir o prato (ignorando subrefs — eles são contados via suas sub-fichas)
function buildShoppingList(dish, scales) {
  const agg = {};
  for (const sf of dish.sub_fichas) {
    const scale = scales[sf.id] || 1;
    for (const ing of (sf.ingredientes || [])) {
      if (ing.subref_id) continue;
      if (ing.qty == null) continue;
      const insumo = findInsumo(ing.insumo_id);
      if (!insumo) continue;
      const [qn, pn] = normalizeQtyPrice(ing, insumo);
      if (qn == null || pn == null) continue;
      const fc = ing.fc || 1;
      const key = ing.insumo_id;
      if (!agg[key]) agg[key] = { name: insumo.name, unit: insumo.unit, qty: 0, cost: 0 };
      agg[key].qty += qn * scale * fc;
      agg[key].cost += qn * pn * scale * fc;
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
          return [s.name, `${norm.text} ${norm.unit}`.trim()];
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
        return [s.name, norm.text, norm.unit, s.cost];
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
