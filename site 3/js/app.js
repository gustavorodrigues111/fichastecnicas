/* ================================================================
   Fichas Técnicas — single-page app (vanilla JS)
   Author: generated for consultoria
   ================================================================ */

const STORAGE_KEY = 'fichas-tecnicas-v1';
let DATA = null;

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
const escapeHtml = s => (s == null ? '' : String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])));
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

// ---------- Data layer ----------
async function loadData() {
  const local = localStorage.getItem(STORAGE_KEY);
  if (local) {
    try { DATA = JSON.parse(local); return; } catch (e) { console.warn('bad local, reloading'); }
  }
  const resp = await fetch('data/data.json');
  DATA = await resp.json();
  persist();
}
function persist() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(DATA));
}
async function resetData() {
  if (!confirm('Restaurar do arquivo original? Todas as edições locais serão perdidas.')) return;
  localStorage.removeItem(STORAGE_KEY);
  const resp = await fetch('data/data.json');
  DATA = await resp.json();
  persist();
  route();
  toast('Dados restaurados do original');
}
function downloadBackup() {
  const blob = new Blob([JSON.stringify(DATA, null, 2)], { type: 'application/json' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = `fichas-backup-${new Date().toISOString().slice(0,10)}.json`;
  a.click();
  URL.revokeObjectURL(url);
  toast('Backup baixado');
}
function restoreBackup(file) {
  const reader = new FileReader();
  reader.onload = e => {
    try {
      const d = JSON.parse(e.target.result);
      if (!d.dishes || !d.insumos) throw new Error('formato inválido');
      DATA = d;
      persist();
      route();
      toast('Backup restaurado');
    } catch (err) { alert('Arquivo inválido: ' + err.message); }
  };
  reader.readAsText(file);
}

// ---------- Cost calculations ----------
function findInsumo(id) { return DATA.insumos.find(i => i.id === id); }
function subfichaCost(sf) {
  let total = 0;
  const rows = sf.ingredientes.map(ing => {
    const insumo = findInsumo(ing.insumo_id);
    const price = insumo ? (insumo.price || 0) : 0;
    // Assumption: price is per "unit" of insumo (e.g. per kg or per unit). Quantity in recipe uses same unit.
    // To handle "g" recipe ingredient while insumo price is per "kg", we normalize:
    const [qNorm, priceNorm] = normalizeQtyPrice(ing, insumo);
    const cost = (qNorm != null && priceNorm != null) ? qNorm * priceNorm : 0;
    total += cost;
    return { ing, insumo, cost };
  });
  return { rows, total };
}
function normalizeQtyPrice(ing, insumo) {
  // Given ingredient quantity + unit vs insumo price unit, normalize so multiplication is correct.
  // Supported conversions: g <-> kg, ml <-> l. Otherwise assume same unit.
  if (!insumo || ing.qty == null) return [null, null];
  const iu = (ing.unit || '').toLowerCase().trim();
  const pu = (insumo.unit || '').toLowerCase().trim();
  const q = ing.qty;
  const p = insumo.price || 0;
  if (iu === pu || !iu || !pu) return [q, p];
  // conversions
  if (iu === 'g' && pu === 'kg') return [q / 1000, p];
  if (iu === 'kg' && pu === 'g') return [q * 1000, p];
  if (iu === 'ml' && (pu === 'l' || pu === 'lt' || pu === 'litro')) return [q / 1000, p];
  if (iu === 'l' && pu === 'ml') return [q * 1000, p];
  // fallback: trust units are equivalent
  return [q, p];
}
function dishCost(dish) {
  const sfCosts = dish.sub_fichas.map(sf => ({ ...subfichaCost(sf), sf }));
  const total = sfCosts.reduce((s, x) => s + x.total, 0);
  // Final sub-ficha often represents per-portion, use its yield to estimate portion count
  const finalSf = dish.sub_fichas[dish.sub_fichas.length - 1];
  const portions = parsePortions(finalSf ? finalSf.rendimento : '');
  const costPerPortion = portions > 0 ? total / portions : 0;
  const markup = dish.markup || 300; // percentage
  const suggestedPrice = costPerPortion * (1 + markup / 100);
  const cmv = suggestedPrice > 0 ? (costPerPortion / suggestedPrice) * 100 : 0;
  return { sfCosts, total, portions, costPerPortion, suggestedPrice, cmv };
}
function parsePortions(rendStr) {
  if (!rendStr) return 1;
  const s = String(rendStr).toLowerCase();
  // Try to find "X unidades", "X porções", "X porção"
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
  // Highlight nav
  $$('.site-nav a').forEach(a => a.classList.remove('active'));
  if (parts.length === 0) {
    $('[data-nav="home"]').classList.add('active');
    renderHome();
  } else if (parts[0] === 'ficha' && parts[1]) {
    renderFicha(parts[1]);
  } else if (parts[0] === 'insumos') {
    $('[data-nav="insumos"]').classList.add('active');
    renderInsumos();
  } else if (parts[0] === 'admin') {
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

// ---------- Views ----------
function renderHome() {
  const app = $('#app');
  app.innerHTML = '';
  const totalSubfichas = DATA.dishes.reduce((s, d) => s + d.sub_fichas.length, 0);
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
    const { costPerPortion, portions } = dishCost(dish);
    const photo = dish.photos && dish.photos[0];
    const card = el('a', { class: 'card-dish', href: `#/ficha/${dish.id}` },
      el('div', { class: 'card-photo' },
        photo ? el('img', { src: photo, alt: dish.name }) : el('span', { class: 'placeholder' }, 'sem foto')
      ),
      el('div', { class: 'card-body' },
        el('h3', {}, dish.name),
        el('div', { class: 'card-meta' },
          el('span', {}, 'Rendimento', el('br'), el('strong', {}, dish.sub_fichas[dish.sub_fichas.length-1]?.rendimento || '—')),
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

  // State for this view
  const state = {
    view: 'trabalho', // 'trabalho' | 'custo'
    activeSf: dish.sub_fichas[dish.sub_fichas.length - 1]?.id || dish.sub_fichas[0]?.id,
  };

  const header = el('div', { class: 'ficha-header' },
    el('a', { class: 'back-link', href: '#/' }, '← Voltar ao cardápio'),
    el('h1', {}, dish.name)
  );
  app.appendChild(header);

  // Gallery
  if (dish.photos && dish.photos.length) {
    const gallery = el('div', { class: 'gallery' });
    dish.photos.forEach(p => gallery.appendChild(el('div', { class: 'photo' }, el('img', { src: p }))));
    app.appendChild(gallery);
  }

  // View toggle
  const toggle = el('div', { class: 'view-toggle' },
    el('button', { 'data-view': 'trabalho' }, 'Ficha de trabalho'),
    el('button', { 'data-view': 'custo' }, 'Ficha de custo')
  );
  app.appendChild(toggle);

  // Tabs
  const tabs = el('div', { class: 'subficha-tabs' });
  dish.sub_fichas.forEach(sf => {
    tabs.appendChild(el('button', { 'data-sf': sf.id }, sf.name));
  });
  app.appendChild(tabs);

  // Body container
  const body = el('div', {});
  app.appendChild(body);

  // Actions (exports)
  const actions = el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn', onclick: () => exportFichaPDF(dish, state) }, 'Exportar PDF'),
    el('button', { class: 'btn', onclick: () => exportFichaXLSX(dish) }, 'Exportar Excel')
  );
  app.appendChild(actions);

  function updateUI() {
    // Toggle states
    $$('.view-toggle button', toggle).forEach(b => b.classList.toggle('active', b.dataset.view === state.view));
    $$('.subficha-tabs button', tabs).forEach(b => b.classList.toggle('active', b.dataset.sf === state.activeSf));
    const sf = dish.sub_fichas.find(s => s.id === state.activeSf) || dish.sub_fichas[0];
    body.innerHTML = '';
    if (state.view === 'trabalho') {
      body.appendChild(renderFichaTrabalho(dish, sf));
    } else {
      body.appendChild(renderFichaCusto(dish, sf));
    }
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

  // Ingredients table
  const tbl = el('table', { class: 'ficha-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', { class: 'num' }, 'Quantidade'),
      el('th', {}, 'Unidade'),
      el('th', {}, 'Observação / Processamento')
    )),
    el('tbody', {}, ...sf.ingredientes.map(ing => el('tr', {},
      el('td', { 'data-label': 'Insumo' }, ing.insumo_name),
      el('td', { class: 'num', 'data-label': 'Quantidade' }, ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—'))),
      el('td', { 'data-label': 'Unidade' }, ing.unit || '—'),
      el('td', { 'data-label': 'Observação' }, ing.observacao || '—')
    )))
  );
  wrap.appendChild(tbl);

  // Modo de preparo
  if (sf.modo_preparo) {
    const sec = el('div', { class: 'ficha-section' },
      el('h3', {}, 'Modo de Preparo'),
      el('div', { class: 'modo-preparo' }, sf.modo_preparo)
    );
    wrap.appendChild(sec);
  }

  // Apresentação (louça)
  if (dish.louca) {
    wrap.appendChild(el('div', { class: 'info-box' },
      el('strong', {}, 'Apresentação / Louça'),
      dish.louca
    ));
  }
  // Equipamentos
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

  // Per-subficha tables
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
      el('tbody', {}, ...rows.map(({ ing, insumo, cost }) => {
        const priceTxt = insumo ? `${fmtBRL(insumo.price || 0)} / ${insumo.unit || '—'}` : '—';
        const qtyTxt = ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—'));
        return el('tr', {},
          el('td', { 'data-label': 'Insumo' }, ing.insumo_name),
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

  // Dish totals
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
  cost.appendChild(el('div', { class: 'stat' },
    el('span', { class: 'stat-label' }, 'Markup (%)'),
    (() => {
      const input = el('input', { type: 'number', min: '0', step: '5', value: String(dish.markup || 300) });
      input.addEventListener('input', () => {
        dish.markup = parseFloat(input.value) || 0;
        persist();
        // Update the numbers in-place
        priceSpan.textContent = fmtBRL(parseFloat(input.value) >= 0 ? (all.costPerPortion * (1 + (parseFloat(input.value) || 0) / 100)) : 0);
        const newPrice = all.costPerPortion * (1 + (parseFloat(input.value) || 0) / 100);
        cmvSpan.textContent = newPrice > 0 ? fmtNum(all.costPerPortion / newPrice * 100, 1) + '%' : '—';
      });
      return input;
    })()
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
      el('p', {}, 'Atualize os preços — os custos das fichas recalculam automaticamente.')
    ),
    el('input', { type: 'search', class: 'insumos-search', placeholder: 'Buscar insumo…' })
  );
  app.appendChild(header);

  const table = el('table', { class: 'insumos-table' },
    el('thead', {}, el('tr', {},
      el('th', {}, 'Insumo'),
      el('th', {}, 'Unidade de referência'),
      el('th', {}, 'Preço (R$)'),
      el('th', {}, 'Usado em')
    ))
  );
  const tbody = el('tbody', {});

  // Count usage
  const usageMap = {};
  DATA.dishes.forEach(d => {
    d.sub_fichas.forEach(sf => {
      sf.ingredientes.forEach(i => {
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
        const tr = el('tr', {},
          el('td', { 'data-label': 'Insumo' }, insumo.name),
          el('td', { 'data-label': 'Unidade' }, (() => {
            const i = el('input', { class: 'unit-input', value: insumo.unit || '', placeholder: 'g' });
            i.addEventListener('change', () => { insumo.unit = i.value.trim(); persist(); });
            return i;
          })()),
          el('td', { 'data-label': 'Preço (R$)' }, (() => {
            const i = el('input', { type: 'number', min: '0', step: '0.01', value: insumo.price || 0 });
            i.addEventListener('input', () => { insumo.price = parseFloat(i.value) || 0; persist(); });
            return i;
          })()),
          el('td', { 'data-label': 'Usado em' }, (usageMap[insumo.id] || 0) + ' receitas')
        );
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
      el('p', {}, 'Adicione, edite ou exclua pratos. Faça upload de até 3 fotos por prato.')
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
        el('p', {}, `${dish.sub_fichas.length} sub-fichas · ${dish.photos?.length || 0} fotos`)
      ),
      el('div', { class: 'dish-admin-actions' },
        el('a', { class: 'btn btn-small', href: `#/ficha/${dish.id}` }, 'Ver'),
        el('a', { class: 'btn btn-small btn-primary', href: `#/admin/edit/${dish.id}` }, 'Editar'),
        el('button', { class: 'btn btn-small btn-danger', onclick: () => deleteDish(dish.id) }, 'Excluir')
      )
    );
    list.appendChild(item);
  });
  panel.appendChild(list);
  app.appendChild(panel);
}
function deleteDish(id) {
  const d = DATA.dishes.find(x => x.id === id);
  if (!d) return;
  if (!confirm(`Excluir "${d.name}"? Essa ação pode ser revertida restaurando um backup.`)) return;
  DATA.dishes = DATA.dishes.filter(x => x.id !== id);
  persist();
  renderAdminList();
  toast('Ficha excluída');
}

// ---------- Admin: edit/new ----------
function renderAdminEdit(dishId) {
  const app = $('#app');
  app.innerHTML = '';
  let dish;
  if (dishId) {
    dish = DATA.dishes.find(d => d.id === dishId);
    if (!dish) { app.appendChild(el('p', {}, 'Prato não encontrado')); return; }
    // work on copy until save
    dish = JSON.parse(JSON.stringify(dish));
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

  // Basic info
  panel.appendChild(el('h2', {}, 'Informações gerais'));
  const basic = el('div', { class: 'form-grid' },
    fieldInput('Nome do prato', 'text', dish.name, v => dish.name = v),
    fieldInput('Louça / apresentação', 'text', dish.louca, v => dish.louca = v),
    fieldInput('Markup sugerido (%)', 'number', dish.markup, v => dish.markup = parseFloat(v) || 0)
  );
  panel.appendChild(basic);

  // Photos
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
            // Compress: draw to canvas at max 1200px wide
            const img = new Image();
            img.onload = () => {
              const maxW = 1200;
              const scale = Math.min(1, maxW / img.width);
              const canvas = document.createElement('canvas');
              canvas.width = img.width * scale;
              canvas.height = img.height * scale;
              canvas.getContext('2d').drawImage(img, 0, 0, canvas.width, canvas.height);
              dish.photos[i] = canvas.toDataURL('image/jpeg', 0.8);
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

  // Equipamentos multi-select
  panel.appendChild(el('h3', {}, 'Equipamentos necessários'));
  const eqBox = el('div', { class: 'equipamentos-select' });
  (DATA.equipamentos_disponiveis || []).forEach(eq => {
    const checked = dish.equipamentos.includes(eq);
    const lbl = el('label', {},
      (() => {
        const i = el('input', { type: 'checkbox' });
        if (checked) i.setAttribute('checked', '');
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

  // Sub-fichas editor
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
      // Ingredients
      const ingHeader = el('h4', {}, 'Insumos');
      box.appendChild(ingHeader);
      const ingList = el('div', {});
      function renderIngs() {
        ingList.innerHTML = '';
        sf.ingredientes.forEach((ing, ingIdx) => {
          const row = el('div', { class: 'ingredient-row' });
          // insumo select with datalist-like behavior
          const nameInput = el('input', { type: 'text', value: ing.insumo_name, placeholder: 'Insumo', list: 'all-insumos' });
          nameInput.addEventListener('input', () => {
            ing.insumo_name = nameInput.value;
            // if matches existing insumo normalize id
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
      // Modo de preparo
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

  // Datalist for ingredient autocomplete
  const dl = el('datalist', { id: 'all-insumos' });
  DATA.insumos.forEach(i => dl.appendChild(el('option', { value: i.name })));
  panel.appendChild(dl);

  // Save / cancel
  const saveBar = el('div', { class: 'ficha-actions' },
    el('button', { class: 'btn btn-primary', onclick: () => saveDish(dish, dishId) }, 'Salvar'),
    el('a', { class: 'btn', href: '#/admin' }, 'Cancelar')
  );
  panel.appendChild(saveBar);

  app.appendChild(panel);
}

function saveDish(dish, originalId) {
  if (!dish.name.trim()) { alert('Nome do prato é obrigatório'); return; }
  dish.id = slugify(dish.name);
  // Register any new insumos
  dish.sub_fichas.forEach(sf => {
    sf.ingredientes.forEach(ing => {
      if (!ing.insumo_name.trim()) return;
      const existing = DATA.insumos.find(i => i.name.toLowerCase() === ing.insumo_name.toLowerCase());
      if (!existing) {
        const newIns = { id: slugify(ing.insumo_name), name: ing.insumo_name.trim(), unit: ing.unit || 'g', price: 0 };
        DATA.insumos.push(newIns);
        ing.insumo_id = newIns.id;
      } else {
        ing.insumo_id = existing.id;
      }
    });
  });
  DATA.insumos.sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()));
  if (originalId) {
    const idx = DATA.dishes.findIndex(d => d.id === originalId);
    if (idx >= 0) DATA.dishes[idx] = dish;
    else DATA.dishes.push(dish);
  } else {
    DATA.dishes.push(dish);
  }
  persist();
  toast('Ficha salva');
  location.hash = `#/ficha/${dish.id}`;
}

// ---------- Helpers for forms ----------
function fieldInput(label, type, value, onChange) {
  const wrap = el('label', { class: 'field' },
    el('span', { class: 'label-text' }, label)
  );
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
  const currentSf = dish.sub_fichas.find(s => s.id === state.activeSf) || dish.sub_fichas[0];

  // Header
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
    // Just the selected sub-ficha
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
      body: currentSf.ingredientes.map(i => [
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
    // Cost view: all sub-fichas with costs, then totals
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
        body: rows.map(({ ing, insumo, cost }) => [
          ing.insumo_name,
          ing.is_qb ? 'Q.B' : (ing.qty != null ? fmtNum(ing.qty, ing.qty % 1 === 0 ? 0 : 2) : (ing.qty_raw || '—')),
          ing.unit || '—',
          insumo ? `${fmtBRL(insumo.price || 0)}/${insumo.unit || '—'}` : '—',
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
    // Totals block
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

  // Summary sheet
  const summaryData = [
    ['FICHA TÉCNICA'],
    ['Prato', dish.name],
    ['Rendimento final', dish.sub_fichas[dish.sub_fichas.length - 1]?.rendimento || '—'],
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

  // One sheet per sub-ficha
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
(async function init() {
  await loadData();
  route();
  $('#btn-download-backup').addEventListener('click', downloadBackup);
  $('#btn-reset').addEventListener('click', resetData);
  $('#file-restore').addEventListener('change', e => {
    if (e.target.files[0]) restoreBackup(e.target.files[0]);
  });
})();
