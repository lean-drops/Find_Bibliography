(function(){
  const $ = (s, c=document) => c.querySelector(s);
  const $$ = (s, c=document) => Array.from(c.querySelectorAll(s));

  // ---------------- Dark-Mode ----------------
  const darkBtn = $('#dark');
  function setDark(on){
    document.documentElement.classList.toggle('dark', !!on);
    try{ localStorage.setItem('chronik_dark', on ? '1':'0'); }catch(e){}
  }
  try{ setDark(localStorage.getItem('chronik_dark') === '1'); }catch(e){}
  if(darkBtn) darkBtn.onclick = () => setDark(!document.documentElement.classList.contains('dark'));

  // ---------------- State ----------------
  let activeGroup = null; // "work" | "series" | "generic" | null
  let activeLabel = null; // label string | null
  let aggQuery = '';      // top search
  let detQuery = '';      // bottom search

  const fltAgg = $('#fltAgg');
  const fltDet = $('#fltDet');
  const clearDet = $('#clearDet');
  const clearGroups = $('#clearGroups');
  const clearLabels = $('#clearLabels');
  const aggCount = $('#aggCount');
  const detCount = $('#detCount');

  const pTitle = $('#pTitle');
  const pBody  = $('#pBody');
  const pClose = $('#pClose');

  function setPanelOpen(on){
    document.documentElement.classList.toggle('panel-open', !!on);
    if(!on) { pTitle.textContent='Info'; pBody.innerHTML="<p>Wähle ein Label…</p>"; }
  }

  // ---------------- Query parsing ----------------
  function parseQuery(q){
    // supports: group:work  label:tschudi  file:jucker  pattern:chronik  plus free text
    const out = { group:null, label:null, file:null, pattern:null, text:[] };
    const toks = (q||'').trim().split(/\s+/).filter(Boolean);
    for(const t of toks){
      const m = t.match(/^(\w+):(.*)$/);
      if(m){
        const k = m[1].toLowerCase(), v = m[2].toLowerCase();
        if(k==='group') out.group = v;
        else if(k==='label') out.label = v;
        else if(k==='file') out.file = v;
        else if(k==='pattern') out.pattern = v;
        else out.text.push(t.toLowerCase());
      }else{
        out.text.push(t.toLowerCase());
      }
    }
    return out;
  }

  // ---------------- Filtering ----------------
  function visibleAggRow(row, q){
    const g = row.dataset.group || '';
    const l = row.dataset.label || '';
    if(activeGroup && g !== activeGroup) return false;
    if(activeLabel && l !== activeLabel) return false;
    if(!q) return true;
    const {group,label,text} = parseQuery(q);
    if(group && g.toLowerCase().indexOf(group)===-1) return false;
    if(label && l.toLowerCase().indexOf(label)===-1) return false;
    if(text.length){
      const hay = (g+' '+l+' '+row.innerText).toLowerCase();
      for(const t of text){ if(hay.indexOf(t)===-1) return false; }
    }
    return true;
  }

  function visibleDetRow(row, q){
    const g = row.dataset.group || '';
    const l = row.dataset.label || '';
    const file = row.children[0]?.innerText || '';
    const page = row.children[1]?.innerText || '';
    const pat  = row.children[4]?.innerText || '';
    const ctx  = row.children[5]?.innerText || '';
    // label/group scoping
    if(activeGroup && g !== activeGroup) return false;
    if(activeLabel && l !== activeLabel) return false;
    if(!q) return true;
    const {group,label,file:ff,pattern,text} = parseQuery(q);
    if(group && g.toLowerCase().indexOf(group)===-1) return false;
    if(label && l.toLowerCase().indexOf(label)===-1) return false;
    if(ff && file.toLowerCase().indexOf(ff)===-1) return false;
    if(pattern && (pat.toLowerCase().indexOf(pattern)===-1 && ctx.toLowerCase().indexOf(pattern)===-1)) return false;
    if(text.length){
      const hay = (file+' '+page+' '+g+' '+l+' '+pat+' '+ctx).toLowerCase();
      for(const t of text){ if(hay.indexOf(t)===-1) return false; }
    }
    return true;
  }

  function applyAgg(){
    const rows = $$('#agg tbody tr');
    let vis = 0;
    rows.forEach(r => { const ok = visibleAggRow(r, aggQuery); r.classList.toggle('hidden', !ok); if(ok) vis++; });
    if(aggCount) aggCount.textContent = String(vis);
    // keep panel consistent
    if(activeLabel) showPanel(activeLabel);
  }

  function applyDet(){
    const rows = $$('#det tbody tr');
    let vis = 0;
    rows.forEach(r => { const ok = visibleDetRow(r, detQuery); r.classList.toggle('hidden', !ok); if(ok) vis++; });
    if(detCount) detCount.textContent = String(vis);
    if(activeLabel) showPanel(activeLabel);
  }

  // ---------------- Panel (stats only) ----------------
  function showPanel(label){
    const s = (window.LABEL_STATS||{})[label] || null;
    pTitle.textContent = label;
    let html = "";
    if(s){
      html += "<div class='kv'><div>Gruppe</div><div class='badge'>"+esc(s.group)+"</div>";
      html += "<div>Mentions</div><div>"+s.mentions+"</div>";
      html += "<div>Dokumente</div><div>"+s.docs+"</div>";
      html += "<div>Weighted</div><div>"+s.weighted+"</div></div>";
    }else{
      html += "<p class='small'>Keine Statistiken gefunden.</p>";
    }
    pBody.innerHTML = html;
  }

  // ---------------- Bindings ----------------
  function selectLabel(label){
    activeLabel = label;
    $$('.chip.label').forEach(c=>c.classList.toggle('active', c.dataset.label===activeLabel));
    setPanelOpen(!!activeLabel);
    applyAgg(); applyDet();
    if(activeLabel) showPanel(activeLabel);
  }

  // group chips
  $$('.chip.group').forEach(ch=>{
    ch.addEventListener('click', ()=>{
      const g = ch.dataset.group;
      activeGroup = (activeGroup===g)?null:g;
      $$('.chip.group').forEach(c=>c.classList.toggle('active', c.dataset.group===activeGroup));
      applyAgg(); applyDet();
    });
  });
  if(clearGroups){ clearGroups.onclick = ()=>{ activeGroup=null; $$('.chip.group').forEach(c=>c.classList.remove('active')); applyAgg(); applyDet(); }; }

  // label chips + label buttons
  $$('.chip.label').forEach(ch=> ch.addEventListener('click', ()=> selectLabel(ch.dataset.label)));
  $$('#agg .lbl, #det .lbl').forEach(b=> b.addEventListener('click', ()=> selectLabel(b.dataset.label)));
  if(clearLabels){ clearLabels.onclick = ()=> selectLabel(null); }
  if(pClose){ pClose.onclick = ()=> selectLabel(null); }

  // search inputs
  if(fltAgg){ fltAgg.oninput = ()=>{ aggQuery = (fltAgg.value||''); applyAgg(); }; }
  if(fltDet){ fltDet.oninput = ()=>{ detQuery = (fltDet.value||''); applyDet(); }; }
  if(clearDet){ clearDet.onclick = ()=>{ detQuery=''; if(fltDet) fltDet.value=''; applyDet(); }; }

  // helpers
  function esc(s){ return String(s).replace(/[&<>"']/g, m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

  // init
  applyAgg(); applyDet();
})();