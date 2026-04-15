import pandas as pd
import json
import sys
from pathlib import Path
 
# ═══════════════════════════════════════════════
# CODIFICACIÓN DE FASES
# ═══════════════════════════════════════════════
ORDEN_FASES = [
    '1F-RR2','2F-RR1','3F-IDA','4F-IDA','5F',
    'REV-1-RR1','REV-2-IDA','REV-3-IDA','REV-4-IDA','REV-5-IDA','REV-6-IDA'
]
 
FASE_A_PANEL = {
    '1F-RR2':    '1f',
    '2F-RR1':    '2f',
    '3F-IDA':    '3f', '3F-VTA':    '3f',
    '4F-IDA':    '4f', '4F-VTA':    '4f',
    '5F':        'fn',
    'REV-1-RR1': 'r1',
    'REV-2-IDA': 'r2', 'REV-2-VTA': 'r2',
    'REV-3-IDA': 'r3', 'REV-3-VTA': 'r3',
    'REV-4-IDA': 'r4', 'REV-4-VTA': 'r4',
    'REV-5-IDA': 'r5', 'REV-5-VTA': 'r5',
    'REV-6-IDA': 'r6', 'REV-6-VTA': 'r6',
}
 
ZONA_CONFIG = {
    1: {'qualify': 5, 'total': 10, 'dest_2f': 'A'},
    2: {'qualify': 4, 'total': 9,  'dest_2f': 'A'},
    3: {'qualify': 4, 'total': 9,  'dest_2f': 'B'},
    4: {'qualify': 4, 'total': 9,  'dest_2f': 'B'},
}
 
ZONE_COLORS = {1:'#4fc3f7', 2:'#81c784', 3:'#ffb74d', 4:'#f06292'}
 
# ═══════════════════════════════════════════════
# LECTURA DE CSVs
# ═══════════════════════════════════════════════
def normalizar_columnas(df):
    """Mapea columnas por contenido parcial para tolerar encoding roto de Google Sheets."""
    # Solo se activa si la columna canónica NO existe ya en el df
    mapa = {
        'N° Partido':           ['partid'],
        'Fase':                 ['fase'],
        'Zona':                 ['zona'],
        'GF':                   ['^gf$'],
        'GC':                   ['^gc$'],
        'PTS Local':            ['pts local', 'pts_local'],
        'PTS Visit.':           ['pts visit', 'pts_visit'],
        'Árbitro':              ['rbitro', 'arbitro'],
        'Penales':              ['penales'],
        'Equipo que convierte': ['convierte'],
        'Jugador':              ['^jugador$'],
        'Tiempo':               ['^tiempo$'],
        'Minuto':               ['^minuto$'],
    }
    import re as _re
    rename = {}
    existing = set(df.columns)
    for col in df.columns:
        col_l = col.lower().strip()
        for canonical, patrones in mapa.items():
            if canonical in existing:
                continue
            for p in patrones:
                if p.startswith('^'):
                    if _re.fullmatch(p[1:-1], col_l):
                        rename[col] = canonical
                        existing.add(canonical)
                        break
                else:
                    if p in col_l:
                        rename[col] = canonical
                        existing.add(canonical)
                        break
            if col in rename:
                break
    if rename:
        df = df.rename(columns=rename)
    return df
 
def leer_carga(path):
    df = pd.read_csv(path, skiprows=1, dtype=str)
    df.columns = df.columns.str.strip()
    df = normalizar_columnas(df)
    for col in ['N° Partido','Fecha','Zona','GF','GC','PTS Local','PTS Visit.']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['Fase'] = df['Fase'].str.strip()
    return df
 
def leer_goles(path):
    df = pd.read_csv(path, skiprows=1, dtype=str)
    df.columns = df.columns.str.strip()
    df = normalizar_columnas(df)
    for col in ['N° Partido','Fecha','Minuto']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df['es_ec'] = df['Jugador'].str.contains(r'\(e/c\)', na=False)
    df['Jugador_limpio'] = df['Jugador'].str.replace(r'\s*\(e/c\)\s*', '', regex=True).str.strip()
    df['es_penal'] = df['Jugador'].str.contains(r'\(p\)', na=False)
    df['Jugador_limpio'] = df['Jugador_limpio'].str.replace(r'\s*\(p\)\s*', '', regex=True).str.strip()
    return df
 
# ═══════════════════════════════════════════════
# DETECCIÓN DE FASES
# ═══════════════════════════════════════════════
def detectar_fases(df):
    jugados = df[df['GF'].notna() & df['GC'].notna()]
    return jugados['Fase'].dropna().unique().tolist()
 
def fase_activa(fases):
    for f in reversed(ORDEN_FASES):
        if f in fases:
            return f
    return fases[0] if fases else '1F-RR2'
 
# ═══════════════════════════════════════════════
# STANDINGS
# ═══════════════════════════════════════════════
def calcular_standings_rr(df, codigos_fase, agrupar_por='Zona'):
    jugados = df[df['GF'].notna() & df['GC'].notna()].copy()
    jugados = jugados[jugados['Fase'].isin(codigos_fase)]
    teams = {}
    partidos_raw = []  # guardamos todos los partidos para h2h
    for _, r in jugados.iterrows():
        grupo = str(r.get(agrupar_por, '?')).strip()
        gf, gc = int(r['GF']), int(r['GC'])
        local    = str(r['Local']).strip()
        visitante= str(r['Visitante']).strip()
        partidos_raw.append({'local': local, 'visitante': visitante,
                             'gf': gf, 'gc': gc, 'grupo': grupo})
        for nombre, my_gf, my_gc in [(local, gf, gc), (visitante, gc, gf)]:
            key = (nombre, grupo)
            if key not in teams:
                teams[key] = {'nombre': nombre, 'zona': grupo,
                              'pj':0,'pg':0,'pe':0,'pp':0,'gf':0,'gc':0,'pts':0}
            t = teams[key]
            t['pj'] += 1; t['gf'] += my_gf; t['gc'] += my_gc
            if my_gf > my_gc:    t['pg'] += 1; t['pts'] += 3
            elif my_gf == my_gc: t['pe'] += 1; t['pts'] += 1
            else:                t['pp'] += 1
 
    by_group = {}
    for (nombre, grupo), t in teams.items():
        if grupo not in by_group:
            by_group[grupo] = []
        by_group[grupo].append(t)
 
    def h2h_stats(equipo, rivales, grupo):
        """Estadísticas solo entre un equipo y un conjunto de rivales (head-to-head)."""
        pts = dg = gf = gf_vis = 0
        for p in partidos_raw:
            if p['grupo'] != grupo:
                continue
            local_ok = p['local'] == equipo and p['visitante'] in rivales
            visit_ok = p['visitante'] == equipo and p['local'] in rivales
            if local_ok:
                my_gf, my_gc = p['gf'], p['gc']
                gf_vis += 0  # local no suma gf visitante
            elif visit_ok:
                my_gf, my_gc = p['gc'], p['gf']
                gf_vis += my_gf  # visitante suma gf visitante
            else:
                continue
            gf += my_gf
            dg += my_gf - my_gc
            if my_gf > my_gc:   pts += 3
            elif my_gf == my_gc: pts += 1
        return (pts, dg, gf, gf_vis)
 
    def ordenar_grupo(lista, grupo):
        """
        Ordena según reglamento Art. 12:
        1. PTS general
        2. Entre empatados: h2h pts → h2h DG → h2h GF → h2h GF visitante
        3. Si persiste empate: DG general → GF general → GF visitante general → sorteo
        Si el empate es parcial (3+ equipos), reinicia el proceso con los que siguen empatados.
        """
        # Primero ordenamos por PTS general
        lista.sort(key=lambda x: (-x['pts'], -(x['gf']-x['gc']), -x['gf']))
 
        # Buscamos grupos de empatados en PTS y aplicamos h2h
        resultado = []
        i = 0
        while i < len(lista):
            # Encontrar todos los que tienen los mismos PTS
            j = i + 1
            while j < len(lista) and lista[j]['pts'] == lista[i]['pts']:
                j += 1
            grupo_empatado = lista[i:j]
 
            if len(grupo_empatado) == 1:
                resultado.append(grupo_empatado[0])
                i = j
                continue
 
            # Hay empate — aplicar h2h entre ellos
            nombres_empatados = {t['nombre'] for t in grupo_empatado}
            grupo_empatado.sort(key=lambda x: (
                *[-v for v in h2h_stats(x['nombre'],
                                        nombres_empatados - {x['nombre']},
                                        grupo)],
                -(x['gf'] - x['gc']),
                -x['gf']
            ))
            resultado.extend(grupo_empatado)
            i = j
 
        return resultado
 
    for g in by_group:
        by_group[g] = ordenar_grupo(by_group[g], g)
 
    return by_group
 
def calcular_standings_1f(df):
    codigos = ['1F-RR2']
    if '1F' in df['Fase'].values:
        codigos.append('1F')
    raw = calcular_standings_rr(df, codigos, 'Zona')
    return {int(k): v for k, v in raw.items() if str(k).isdigit()}
 
def calcular_standings_2f(df):
    return calcular_standings_rr(df, ['2F-RR1'], 'Zona')
 
def calcular_standings_rev1(df):
    return calcular_standings_rr(df, ['REV-1-RR1'], 'Zona')
 
# ═══════════════════════════════════════════════
# MEJOR 5TO
# ═══════════════════════════════════════════════
def get_mejor_5to(standings):
    candidates = []
    for z in [2, 3, 4]:
        data = standings.get(z, [])
        if len(data) >= 5:
            t = data[4]
            candidates.append({**t, 'from_zone': z, 'zona': z})
    if not candidates:
        return None
    # Desempate entre 5tos: PTS → DG → GF → GF visitante (no hay h2h entre zonas distintas)
    candidates.sort(key=lambda x: (-x['pts'], -(x['gf']-x['gc']), -x['gf']))
    return candidates[0]
 
# ═══════════════════════════════════════════════
# GOLEADORES INDIVIDUALES
# ═══════════════════════════════════════════════
def calcular_goleadores(dg):
    df = dg[~dg['es_ec']].copy()
    goleadores = df.groupby('Jugador_limpio').agg(
        goles=('Jugador_limpio','count'),
        equipo=('Equipo que convierte', lambda x: x.mode()[0] if len(x) else ''),
        penales=('es_penal', 'sum')
    ).reset_index()
    goleadores = goleadores.sort_values('goles', ascending=False)
    result = []
    for _, r in goleadores.head(20).iterrows():
        result.append({
            'jugador': r['Jugador_limpio'],
            'equipo': r['equipo'],
            'goles': int(r['goles']),
            'penales': int(r['penales'])
        })
    return result
 
# ═══════════════════════════════════════════════
# STATS DE EQUIPOS (NUEVO)
# ═══════════════════════════════════════════════
def calcular_stats_equipos(df_carga, standings_1f):
    """
    Stats completas por equipo para la tabla general.
    Acumula TODAS las fases de round robin (1F-RR2, 2F-RR1, REV-1-RR1).
    Calcula fase_actual = la instancia más avanzada que jugó cada equipo.
    """
    # Fases RR ordenadas de menor a mayor avance
    FASES_RR    = ['1F-RR2', '1F', '2F-RR1', 'REV-1-RR1']
    FASE_ORDEN  = {'1F-RR2':1, '1F':1, '2F-RR1':2, 'REV-1-RR1':2}
    FASE_LABEL  = {'1F-RR2':'1F', '1F':'1F', '2F-RR1':'2F', 'REV-1-RR1':'REV-1'}
 
    jugados = df_carga[
        df_carga['GF'].notna() & df_carga['GC'].notna() &
        df_carga['Fase'].isin(FASES_RR)
    ].copy()
 
    if jugados.empty:
        return []
 
    jugados['GF']    = jugados['GF'].astype(int)
    jugados['GC']    = jugados['GC'].astype(int)
    jugados['Fecha'] = pd.to_numeric(jugados['Fecha'], errors='coerce').fillna(0).astype(int)
 
    # Posición y zona desde standings 1F (base siempre)
    pos_map  = {}
    zona_map = {}
    for zona, equipos in standings_1f.items():
        for i, t in enumerate(equipos):
            pos_map[t['nombre']]  = i + 1
            zona_map[t['nombre']] = int(zona)
 
    stats = {}
 
    for _, r in jugados.sort_values('Fecha').iterrows():
        loc  = str(r['Local']).strip()
        vis  = str(r['Visitante']).strip()
        gf   = int(r['GF'])
        gc   = int(r['GC'])
        fase = str(r['Fase']).strip()
 
        for eq, cond, egf, egc in [(loc,'L',gf,gc), (vis,'V',gc,gf)]:
            if eq not in stats:
                stats[eq] = {
                    'nombre':   eq,
                    'zona':     zona_map.get(eq, 0),
                    'pos':      pos_map.get(eq, 0),
                    'fase_max': 0,        # orden numérico de la fase más avanzada
                    'fase_actual': '1F',  # label para mostrar
                    'pj':0,'pg':0,'pe':0,'pp':0,
                    'gf':0,'gc':0,'pts':0,
                    'loc_pj':0,'loc_pts':0,'loc_gf':0,'loc_gc':0,
                    'loc_pg':0,'loc_pe':0,'loc_pp':0,
                    'vis_pj':0,'vis_pts':0,'vis_gf':0,'vis_gc':0,
                    'vis_pg':0,'vis_pe':0,'vis_pp':0,
                    'racha':[],
                }
 
            t = stats[eq]
 
            # Actualizar fase más avanzada
            orden = FASE_ORDEN.get(fase, 0)
            if orden > t['fase_max']:
                t['fase_max']    = orden
                t['fase_actual'] = FASE_LABEL.get(fase, fase)
 
            # Acumular stats
            t['pj']+=1; t['gf']+=egf; t['gc']+=egc
            res = 'G' if egf>egc else ('E' if egf==egc else 'P')
            if res=='G':   t['pg']+=1; t['pts']+=3
            elif res=='E': t['pe']+=1; t['pts']+=1
            else:          t['pp']+=1
            t['racha'].append(res)
 
            pts_cond = 3 if egf>egc else (1 if egf==egc else 0)
            if cond=='L':
                t['loc_pj']+=1; t['loc_gf']+=egf
                t['loc_gc']+=egc; t['loc_pts']+=pts_cond
                if res=='G': t['loc_pg']+=1
                elif res=='E': t['loc_pe']+=1
                else: t['loc_pp']+=1
            else:
                t['vis_pj']+=1; t['vis_gf']+=egf
                t['vis_gc']+=egc; t['vis_pts']+=pts_cond
                if res=='G': t['vis_pg']+=1
                elif res=='E': t['vis_pe']+=1
                else: t['vis_pp']+=1
 
    # Calcular métricas derivadas
    result = []
    for eq, t in stats.items():
        pj = t['pj']
        t['dg']     = t['gf'] - t['gc']
        t['gf_pj']  = round(t['gf'] / pj, 2) if pj else 0.0
        t['gc_pj']  = round(t['gc'] / pj, 2) if pj else 0.0
        t['pts_pj'] = round(t['pts'] / pj, 2) if pj else 0.0
        t['rdt']    = round(t['pts'] / (pj * 3) * 100) if pj else 0
        t['rdt_loc']= round(t['loc_pts'] / (t['loc_pj'] * 3) * 100) if t['loc_pj'] else None
        t['rdt_vis']= round(t['vis_pts'] / (t['vis_pj'] * 3) * 100) if t['vis_pj'] else None
        t['pct_g']    = round(t['pg'] / pj * 100) if pj else 0
        t['pct_e']    = round(t['pe'] / pj * 100) if pj else 0
        t['pct_p']    = round(t['pp'] / pj * 100) if pj else 0
        t['loc_pct_g']= round(t['loc_pg'] / t['loc_pj'] * 100) if t['loc_pj'] else None
        t['loc_pct_e']= round(t['loc_pe'] / t['loc_pj'] * 100) if t['loc_pj'] else None
        t['loc_pct_p']= round(t['loc_pp'] / t['loc_pj'] * 100) if t['loc_pj'] else None
        t['vis_pct_g']= round(t['vis_pg'] / t['vis_pj'] * 100) if t['vis_pj'] else None
        t['vis_pct_e']= round(t['vis_pe'] / t['vis_pj'] * 100) if t['vis_pj'] else None
        t['vis_pct_p']= round(t['vis_pp'] / t['vis_pj'] * 100) if t['vis_pj'] else None
        t['valla']  = (t['gc'] == 0 and pj > 0)
        t['racha3'] = t['racha'][-5:]  # últimos 5
        result.append(t)
 
    return result
 
 
def calcular_racha(df_carga, standings_1f, n=5):
    """Últimos N resultados de cada equipo, ordenados por fecha."""
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    jugados = jugados.sort_values('Fecha')
    historial = {}
    for _, r in jugados.iterrows():
        loc = str(r['Local']).strip()
        vis = str(r['Visitante']).strip()
        gf, gc = int(r['GF']), int(r['GC'])
        historial.setdefault(loc, []).append('G' if gf>gc else ('E' if gf==gc else 'P'))
        historial.setdefault(vis, []).append('G' if gc>gf else ('E' if gc==gf else 'P'))
    # Encontrar zona de cada equipo
    zona_eq = {}
    for zona, equipos in standings_1f.items():
        for t in equipos:
            zona_eq[t['nombre']] = zona
    result = []
    for nombre, h in historial.items():
        result.append({
            'nombre': nombre,
            'zona':   zona_eq.get(nombre, 0),
            'racha':  h[-n:],
            'pj':     len(h),
        })
    result.sort(key=lambda x: (x['zona'], x['nombre']))
    return result
 
def calcular_local_visitante(df_carga, standings_1f):
    """Rendimiento como local vs visitante por equipo."""
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    zona_eq = {}
    for zona, equipos in standings_1f.items():
        for t in equipos:
            zona_eq[t['nombre']] = zona
    stats = {}
    for _, r in jugados.iterrows():
        loc = str(r['Local']).strip()
        vis = str(r['Visitante']).strip()
        gf, gc = int(r['GF']), int(r['GC'])
        for eq, es_local, egf, egc in [(loc,True,gf,gc),(vis,False,gc,gf)]:
            if eq not in stats:
                stats[eq] = {'nombre':eq,'zona':zona_eq.get(eq,0),
                             'loc_pj':0,'loc_pts':0,'loc_gf':0,'loc_gc':0,
                             'vis_pj':0,'vis_pts':0,'vis_gf':0,'vis_gc':0}
            t = stats[eq]
            pts = 3 if egf>egc else (1 if egf==egc else 0)
            if es_local:
                t['loc_pj']+=1; t['loc_pts']+=pts; t['loc_gf']+=egf; t['loc_gc']+=egc
            else:
                t['vis_pj']+=1; t['vis_pts']+=pts; t['vis_gf']+=egf; t['vis_gc']+=egc
    result = list(stats.values())
    result.sort(key=lambda x: (-(x['loc_pts']+x['vis_pts']), x['nombre']))
    return result
 
def calcular_tramos_goles(df_goles):
    """Goles por tramo de minuto."""
    df = df_goles.copy()
    df['Minuto'] = pd.to_numeric(df['Minuto'], errors='coerce')
    df['Tiempo'] = df['Tiempo'].astype(str).str.strip()
    df['min_abs'] = df.apply(
        lambda r: r['Minuto'] if r['Tiempo']=='1T' else r['Minuto']+45, axis=1)
    tramos = [('1-15',1,15),('16-30',16,30),('31-45',31,45),
              ('46-60',46,60),('61-75',61,75),('76-90',76,95)]
    result = []
    total = len(df)
    for label, a, b in tramos:
        n = int(((df['min_abs']>=a) & (df['min_abs']<=b)).sum())
        result.append({'label':label,'goles':n,'pct':round(n/total*100) if total else 0})
    return result
 
def calcular_variedad_scoring(df_goles):
    """Equipos con más goleadores distintos."""
    df = df_goles[~df_goles['Jugador'].str.contains(r'\(e/c\)', na=False)].copy()
    df['jugador_l'] = df['Jugador'].str.replace(r'\s*\(e?/c?\)\s*','',regex=True).str.strip()
    distintos = (df.groupby('Equipo que convierte')['jugador_l']
                   .nunique()
                   .sort_values(ascending=False)
                   .reset_index())
    distintos.columns = ['equipo','goleadores_distintos']
    return distintos.to_dict('records')
 
def calcular_global_loc_vis(df_carga):
    """Resumen global local vs visitante."""
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    jugados['GF'] = jugados['GF'].astype(int)
    jugados['GC'] = jugados['GC'].astype(int)
    total = len(jugados)
    loc_g = int((jugados['GF'] > jugados['GC']).sum())
    emp   = int((jugados['GF'] == jugados['GC']).sum())
    vis_g = int((jugados['GF'] < jugados['GC']).sum())
    return {
        'total': total,
        'local_gana': loc_g,
        'empate':     emp,
        'visit_gana': vis_g,
        'goles_loc':  int(jugados['GF'].sum()),
        'goles_vis':  int(jugados['GC'].sum()),
        'pct_loc':    round(loc_g/total*100) if total else 0,
        'pct_vis':    round(vis_g/total*100) if total else 0,
    }
 
def calcular_perfiles_equipos(df_carga, df_goles, standings_1f):
    """Genera perfil completo de cada equipo para el panel de click."""
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    jugados['GF']    = jugados['GF'].astype(int)
    jugados['GC']    = jugados['GC'].astype(int)
    jugados['Fecha'] = jugados['Fecha'].astype(int)
    jugados['Zona']  = jugados['Zona'].astype(int)
 
    # Zona y posición de cada equipo
    zona_map = {}
    pos_map  = {}
    for zona, equipos in standings_1f.items():
        for i, t in enumerate(equipos):
            zona_map[t['nombre']] = int(zona)
            pos_map[t['nombre']]  = i + 1
 
    # Goles válidos
    dg = df_goles.copy()
    dg['es_ec']     = dg['Jugador'].str.contains(r'\(e/c\)', na=False)
    dg['es_penal']  = dg['Jugador'].str.contains(r'\(p\)', na=False)
    dg['jugador_l'] = (dg['Jugador']
                       .str.replace(r'\s*\(e/c\)\s*','',regex=True)
                       .str.replace(r'\s*\(p\)\s*','',regex=True)
                       .str.strip())
 
    perfiles = {}
    for eq in sorted(zona_map.keys()):
        # Partidos del equipo
        partidos = []
        loc_pj=loc_pts=loc_gf=loc_gc = 0
        vis_pj=vis_pts=vis_gf=vis_gc = 0
        racha = []
 
        for _, r in jugados.sort_values('Fecha').iterrows():
            loc = str(r['Local']).strip()
            vis = str(r['Visitante']).strip()
            gf, gc, fecha = r['GF'], r['GC'], r['Fecha']
 
            if loc == eq:
                res = 'G' if gf>gc else ('E' if gf==gc else 'P')
                pts = 3 if gf>gc else (1 if gf==gc else 0)
                partidos.append({'f':fecha,'cond':'LOCAL','rival':vis,
                                 'gf':gf,'gc':gc,'res':res,
                                 'texto':f"{loc} {gf}-{gc} {vis}"})
                loc_pj+=1; loc_pts+=pts; loc_gf+=gf; loc_gc+=gc
                racha.append(res)
            elif vis == eq:
                res = 'G' if gc>gf else ('E' if gc==gf else 'P')
                pts = 3 if gc>gf else (1 if gc==gf else 0)
                partidos.append({'f':fecha,'cond':'VISIT','rival':loc,
                                 'gf':gc,'gc':gf,'res':res,
                                 'texto':f"{loc} {gf}-{gc} {vis}"})
                vis_pj+=1; vis_pts+=pts; vis_gf+=gc; vis_gc+=gf
                racha.append(res)
 
        # Goleadores del equipo
        sub = dg[(dg['Equipo que convierte']==eq) & (~dg['es_ec'])]
        goleadores = (sub.groupby('jugador_l')
                        .agg(goles=('jugador_l','count'), penales=('es_penal','sum'))
                        .reset_index()
                        .sort_values('goles', ascending=False))
        gol_list = []
        for _, gr in goleadores.iterrows():
            gol_list.append({
                'jugador': gr['jugador_l'],
                'goles':   int(gr['goles']),
                'penales': int(gr['penales'])
            })
 
        # Stats totales desde standings
        st = next((t for t in standings_1f.get(zona_map.get(eq,1),[])
                   if t['nombre']==eq), {})
 
        perfiles[eq] = {
            'nombre':   eq,
            'zona':     zona_map.get(eq, 0),
            'pos':      pos_map.get(eq, 0),
            'pj':       st.get('pj',0),
            'pts':      st.get('pts',0),
            'pg':       st.get('pg',0),
            'pe':       st.get('pe',0),
            'pp':       st.get('pp',0),
            'gf':       st.get('gf',0),
            'gc':       st.get('gc',0),
            'dg':       st.get('gf',0)-st.get('gc',0),
            'loc_pj':   loc_pj, 'loc_pts': loc_pts,
            'loc_gf':   loc_gf, 'loc_gc':  loc_gc,
            'vis_pj':   vis_pj, 'vis_pts': vis_pts,
            'vis_gf':   vis_gf, 'vis_gc':  vis_gc,
            'racha':    racha[-5:],
            'partidos': partidos,
            'goleadores': gol_list,
        }
 
    return perfiles
 
 
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna()].copy()
    jugados['total_goles'] = jugados['GF'].astype(int) + jugados['GC'].astype(int)
    jugados = jugados.sort_values('total_goles', ascending=False).head(top_n)
    result = []
    for _, r in jugados.iterrows():
        result.append({
            'local':    str(r['Local']).strip(),
            'visitante':str(r['Visitante']).strip(),
            'gf':       int(r['GF']),
            'gc':       int(r['GC']),
            'total':    int(r['total_goles']),
            'fecha':    int(r['Fecha']) if pd.notna(r['Fecha']) else 0,
        })
    return result
 
# ═══════════════════════════════════════════════
# DESCENSOS
# ═══════════════════════════════════════════════
def calcular_descensos(df_carga, standings_1f):
    if 'REV-1-RR1' not in df_carga['Fase'].fillna('').values:
        return None
    pts_1f = {}
    for zona_data in standings_1f.values():
        for t in zona_data:
            pts_1f[t['nombre']] = {'pts': t['pts'], 'pj': t['pj'], 'zona_1f': t['zona']}
    standings_rev1 = calcular_standings_rev1(df_carga)
    resultado = {'zona_a': [], 'zona_b': [], 'descienden': []}
    for grupo in ['A', 'B']:
        equipos_rev = standings_rev1.get(grupo, [])
        tabla = []
        for t in equipos_rev:
            nombre = t['nombre']
            base = pts_1f.get(nombre, {'pts': 0, 'pj': 0})
            pts_total = base['pts'] + t['pts']
            pj_total  = base['pj']  + t['pj']
            promedio  = round(pts_total / pj_total, 4) if pj_total > 0 else 0
            tabla.append({
                'nombre':    nombre,
                'zona_rev':  grupo,
                'zona_1f':   base.get('zona_1f', '?'),
                'pts_1f':    base['pts'],
                'pj_1f':     base['pj'],
                'pts_rev':   t['pts'],
                'pj_rev':    t['pj'],
                'pts_total': pts_total,
                'pj_total':  pj_total,
                'promedio':  promedio,
            })
        if grupo == 'A':
            tabla.sort(key=lambda x: x['promedio'])
        else:
            tabla.sort(key=lambda x: x['pts_total'])
        resultado[f'zona_{grupo.lower()}'] = tabla
        top5_rev = set(t['nombre'] for t in equipos_rev[:5])
        peores = [t for t in tabla if t['nombre'] not in top5_rev][:2]
        if len(peores) < 2:
            peores = tabla[:2]
        resultado['descienden'].extend(peores)
    return resultado
 
# ═══════════════════════════════════════════════
# STATS GLOBALES
# ═══════════════════════════════════════════════
def calcular_stats(df_carga, df_goles):
    fases_con_datos = detectar_fases(df_carga)
    fa = fase_activa(fases_con_datos)
    jugados_1f = df_carga[
        df_carga['GF'].notna() & df_carga['GC'].notna() &
        (df_carga['Fase'].isin(['1F-RR2', '1F']))
    ]
    fechas_1f = sorted(jugados_1f['Fecha'].dropna().unique().tolist())
    fecha_actual = int(max(fechas_1f)) if fechas_1f else 0
    paneles_disponibles = set()
    for f in fases_con_datos:
        p = FASE_A_PANEL.get(f)
        if p:
            paneles_disponibles.add(p)
    return {
        'fase_activa':        fa,
        'fecha_actual':       fecha_actual,
        'fecha_total':        18,
        'partidos_jugados':   len(jugados_1f),
        'total_goles':        len(df_goles),
        'fases_disponibles':  fases_con_datos,
        'paneles_disponibles': list(paneles_disponibles),
    }
 
# ═══════════════════════════════════════════════
# GOLEADORES FULL (con zona)
# ═══════════════════════════════════════════════
def calcular_goleadores_full(df_carga, df_goles):
    zona_p = {}
    for _, r in df_carga.iterrows():
        nro = r.get('N° Partido')
        if pd.notna(nro):
            zona_p[int(nro)] = int(r['Zona']) if pd.notna(r.get('Zona')) else 0
 
    df = df_goles.copy()
    df['N° Partido'] = pd.to_numeric(df['N° Partido'], errors='coerce')
    df['zona'] = df['N° Partido'].apply(
        lambda x: zona_p.get(int(x), 0) if pd.notna(x) else 0)
    df = df[~df['Jugador'].str.contains(r'\(e/c\)', na=False)].copy()
    df['jugador_l'] = (df['Jugador']
        .str.replace(r'\s*\(e/c\)\s*', '', regex=True)
        .str.replace(r'\s*\(p\)\s*', '', regex=True)
        .str.strip())
    df['es_penal'] = df['Jugador'].str.contains(r'\(p\)', na=False)
 
    result = []
    for (jug, eq), grp in df.groupby(['jugador_l', 'Equipo que convierte']):
        zona = int(grp['zona'].mode()[0]) if len(grp) else 0
        result.append({
            'jugador': jug,
            'equipo':  eq,
            'zona':    zona,
            'goles':   len(grp),
            'penales': int(grp['es_penal'].sum()),
        })
    result.sort(key=lambda x: (-x['goles'], x['jugador']))
    return result
 
 
def render_goleadores_tab(data):
    import json as _json
    from collections import defaultdict
 
    goleadores = data.get('goleadores_full', [])
    stats_eq   = data.get('stats_equipos', [])
 
    ZC = {1:'#29b6f6', 2:'#66bb6a', 3:'#ff7043', 4:'#f06292'}
 
    # Goleadores distintos por equipo
    dist_map = defaultdict(set)
    for g in goleadores:
        dist_map[g['equipo']].add(g['jugador'])
 
    equipos = []
    for t in stats_eq:
        if t['pj'] > 0:
            equipos.append({
                'nombre':    t['nombre'],
                'zona':      t['zona'],
                'gf':        t['gf'],
                'gf_pj':     round(t['gf_pj'], 2),
                'distintos': len(dist_map.get(t['nombre'], set())),
            })
    equipos.sort(key=lambda x: (-x['gf'], -x['gf_pj']))
 
    gol_js = _json.dumps(goleadores, ensure_ascii=False, default=str)
    eq_js  = _json.dumps(equipos,    ensure_ascii=False, default=str)
    zc_js  = _json.dumps(ZC)
 
    filtros = (
        '<div style="display:flex;gap:4px;flex-wrap:wrap;margin-bottom:12px">'
        '<button class="tbtn active" data-gz="GLOBAL">GLOBAL</button>'
        '<button class="tbtn" data-gz="1" style="color:#29b6f6;border-color:#29b6f6">Z1</button>'
        '<button class="tbtn" data-gz="2" style="color:#66bb6a;border-color:#66bb6a">Z2</button>'
        '<button class="tbtn" data-gz="3" style="color:#ff7043;border-color:#ff7043">Z3</button>'
        '<button class="tbtn" data-gz="4" style="color:#f06292;border-color:#f06292">Z4</button>'
        '<button class="tbtn" data-gz="EQUIPOS">EQUIPOS</button>'
        '</div>'
    )
 
    contenedor = '<div id="gz-lista" style="display:flex;flex-direction:column"></div>'
 
    js = f"""<script>
(function(){{
  var GOL={gol_js};
  var EQ={eq_js};
  var ZC={zc_js};
  var vista='GLOBAL';
 
  document.querySelectorAll('.tbtn[data-gz]').forEach(function(b){{
    b.addEventListener('click',function(){{
      document.querySelectorAll('.tbtn[data-gz]').forEach(function(x){{x.classList.remove('active');}});
      this.classList.add('active');
      vista=this.dataset.gz;
      render();
    }});
  }});
 
  var BD='border-bottom:1px solid var(--s3)';
  var ROW='display:flex;align-items:center;gap:10px;padding:9px 0;'+BD;
  var POS='font-family:var(--mono);font-size:11px;color:var(--tn);width:18px;text-align:right;flex-shrink:0';
  var INFO='flex:1;min-width:0';
  var NAME='font-size:13px;font-weight:500;color:var(--t1);white-space:nowrap;overflow:hidden;text-overflow:ellipsis';
  var SUB='font-size:10px;color:var(--t3);margin-top:2px';
  var NUM_BIG='font-family:var(--mono);font-size:20px;font-weight:500;color:var(--t1);line-height:1';
  var NUM_SUB='font-size:9px;color:var(--t3);text-align:right;margin-top:2px';
 
  function render(){{
    var lista=document.getElementById('gz-lista');
    var html='';
 
    if(vista==='EQUIPOS'){{
      EQ.forEach(function(t,i){{
        var zc=ZC[t.zona]||'#888';
        var sub='<span style="color:'+zc+'">Z'+t.zona+'</span>'+' · '+t.gf_pj.toFixed(2)+'/PJ · '+t.distintos+' goleadores';
        html+='<div style="'+ROW+'">'+
          '<div style="'+POS+'">'+( i+1)+'</div>'+
          '<div style="'+INFO+'">'+
            '<div style="'+NAME+'">'+t.nombre+'</div>'+
            '<div style="'+SUB+'">'+sub+'</div>'+
          '</div>'+
          '<div style="flex-shrink:0;text-align:right">'+
            '<div style="'+NUM_BIG+'">'+t.gf+'</div>'+
            '<div style="'+NUM_SUB+'">goles</div>'+
          '</div>'+
        '</div>';
      }});
    }} else {{
      var filas=vista==='GLOBAL'?GOL:GOL.filter(function(g){{return String(g.zona)===vista;}});
      filas.forEach(function(g,i){{
        var zc=ZC[g.zona]||'#888';
        var sub=g.equipo+' · <span style="color:'+zc+'">Z'+g.zona+'</span>';
        var pen=g.penales>0?'<div style="'+NUM_SUB+'">'+g.penales+'p</div>':'<div style="'+NUM_SUB+'"> </div>';
        html+='<div style="'+ROW+'">'+
          '<div style="'+POS+'">'+( i+1)+'</div>'+
          '<div style="'+INFO+'">'+
            '<div style="'+NAME+'">'+g.jugador+'</div>'+
            '<div style="'+SUB+'">'+sub+'</div>'+
          '</div>'+
          '<div style="flex-shrink:0;text-align:right">'+
            '<div style="'+NUM_BIG+'">'+g.goles+'</div>'+
            pen+
          '</div>'+
        '</div>';
      }});
    }}
    lista.innerHTML=html;
  }}
  render();
}})();
</script>"""
 
    return filtros + contenedor + js
 
# ═══════════════════════════════════════════════
# ARMAR DATOS COMPLETOS
# ═══════════════════════════════════════════════
def armar_datos(df_carga, df_goles):
    stats        = calcular_stats(df_carga, df_goles)
    fases        = stats['fases_disponibles']
    standings_1f = calcular_standings_1f(df_carga)
    mejor_5to    = get_mejor_5to(standings_1f)
    goleadores   = calcular_goleadores(df_goles)
    mejor_5to_nombre = mejor_5to["nombre"] if mejor_5to else ""
 
    # Stats de equipos y métricas avanzadas
    perfiles             = calcular_perfiles_equipos(df_carga, df_goles, standings_1f)
    stats_equipos        = calcular_stats_equipos(df_carga, standings_1f)
    partidos_destacados  = calcular_partidos_destacados(df_carga)
    racha                = calcular_racha(df_carga, standings_1f)
    local_visitante      = calcular_local_visitante(df_carga, standings_1f)
    tramos_goles         = calcular_tramos_goles(df_goles)
    variedad_scoring     = calcular_variedad_scoring(df_goles)
    global_loc_vis       = calcular_global_loc_vis(df_carga)
    penales              = int(df_goles['es_penal'].sum())
    t1_goles             = int((df_goles['Tiempo'].astype(str).str.strip()=='1T').sum())
    t2_goles             = int((df_goles['Tiempo'].astype(str).str.strip()=='2T').sum())
    goles_por_tramo_eq   = calcular_goles_tramo_equipos(df_carga, df_goles, standings_1f)
 
    # Segunda Fase
    if "2F-RR1" in fases:
        standings_2f = calcular_standings_2f(df_carga)
        zona_a_2f = [{**t, "src":"2F"} for t in standings_2f.get("A", [])]
        zona_b_2f = [{**t, "src":"2F"} for t in standings_2f.get("B", [])]
    else:
        zona_a_2f = [
            *[{**t, "zona":1, "src":f"Z1·{i+1}°"} for i,t in enumerate((standings_1f.get(1,[]))[:5])],
            *[{**t, "zona":2, "src":f"Z2·{i+1}°"} for i,t in enumerate((standings_1f.get(2,[]))[:4])],
        ]
        zona_b_2f = [
            *[{**t, "zona":3, "src":f"Z3·{i+1}°"} for i,t in enumerate((standings_1f.get(3,[]))[:4])],
            *[{**t, "zona":4, "src":f"Z4·{i+1}°"} for i,t in enumerate((standings_1f.get(4,[]))[:4])],
        ]
        if mejor_5to:
            zona_b_2f.append({**mejor_5to, "src": f"Z{mejor_5to['from_zone']}·5°★"})
 
    # Cruces 3F
    a, b = zona_a_2f, zona_b_2f
    cruces_3f = []
    if len(a) >= 4 and len(b) >= 4:
        cruces_3f = [
            {"label":"Partido 1","cross":"1A vs 4B","home":b[3]["nombre"],"home_seed":"4B","away":a[0]["nombre"],"away_seed":"1A","home_zona":b[3].get("zona",3),"away_zona":a[0].get("zona",1)},
            {"label":"Partido 2","cross":"2A vs 3B","home":b[2]["nombre"],"home_seed":"3B","away":a[1]["nombre"],"away_seed":"2A","home_zona":b[2].get("zona",3),"away_zona":a[1].get("zona",1)},
            {"label":"Partido 3","cross":"1B vs 4A","home":a[3]["nombre"],"home_seed":"4A","away":b[0]["nombre"],"away_seed":"1B","home_zona":a[3].get("zona",1),"away_zona":b[0].get("zona",3)},
            {"label":"Partido 4","cross":"2B vs 3A","home":a[2]["nombre"],"home_seed":"3A","away":b[1]["nombre"],"away_seed":"2B","home_zona":a[2].get("zona",1),"away_zona":b[1].get("zona",3)},
        ]
 
    # Reválida 1E
    if "REV-1-RR1" in fases:
        standings_rev1 = calcular_standings_rev1(df_carga)
        rev_a = [{**t, "src":"REV"} for t in standings_rev1.get("A", [])]
        rev_b = [{**t, "src":"REV"} for t in standings_rev1.get("B", [])]
    else:
        z2_data = standings_1f.get(2, [])
        z2_5th  = z2_data[4] if len(z2_data) >= 5 else None
        rev_a = [
            *[{**t, "zona":1, "src":f"Z1·{i+6}°"} for i,t in enumerate((standings_1f.get(1,[]))[5:])],
            *([{**z2_5th, "zona":2, "src":"Z2·5°"}] if z2_5th and z2_5th["nombre"] != mejor_5to_nombre else []),
            *[{**t, "zona":2, "src":f"Z2·{i+6}°"} for i,t in enumerate((standings_1f.get(2,[]))[5:]) if t["nombre"] != mejor_5to_nombre],
        ]
        rev_b = [
            *[{**t, "zona":3, "src":f"Z3·{i+5}°"} for i,t in enumerate((standings_1f.get(3,[]))[4:]) if t["nombre"] != mejor_5to_nombre],
            *[{**t, "zona":4, "src":f"Z4·{i+5}°"} for i,t in enumerate((standings_1f.get(4,[]))[4:]) if t["nombre"] != mejor_5to_nombre],
        ]
 
    descensos    = calcular_descensos(df_carga, standings_1f)
    stats_duras  = calcular_stats_duras(df_carga, df_goles)
 
    return {
        "stats":               stats,
        "standings_1f":        {str(z): v for z,v in standings_1f.items()},
        "mejor_5to":           mejor_5to,
        "zona_a_2f":           zona_a_2f,
        "zona_b_2f":           zona_b_2f,
        "cruces_3f":           cruces_3f,
        "rev_a_1e":            rev_a,
        "rev_b_1e":            rev_b,
        "descensos":           descensos,
        "goleadores":          goleadores,
        "perfiles":            perfiles,
        "stats_equipos":       stats_equipos,
        "partidos_destacados": partidos_destacados,
        "racha":               racha,
        "local_visitante":     local_visitante,
        "tramos_goles":        tramos_goles,
        "variedad_scoring":    variedad_scoring,
        "global_loc_vis":      global_loc_vis,
        "penales":             penales,
        "t1_goles":            t1_goles,
        "t2_goles":            t2_goles,
        "tiene_2f_real":       "2F-RR1" in fases,
        "tiene_rev1_real":     "REV-1-RR1" in fases,
        "stats_duras":         stats_duras,
        "goles_por_tramo_eq":  goles_por_tramo_eq,
    }
 
# ═══════════════════════════════════════════════
# RENDER — PRIMERA FASE
# ═══════════════════════════════════════════════
def chip_1f(zona, pos, mejor_5to_nombre, nombre):
    if zona == 1 and pos <= 5: return '<span class="chip c-a">2F·A</span>'
    if zona == 2 and pos <= 4: return '<span class="chip c-a">2F·A</span>'
    if zona in [3,4] and pos <= 4: return '<span class="chip c-b">2F·B</span>'
    if nombre == mejor_5to_nombre: return '<span class="chip c-b5">★5to</span>'
    # REV-A: Z1 pos 6-10, Z2 pos 5-9
    if zona in [1, 2]: return '<span class="chip c-ra">REV·A</span>'
    # REV-B: Z3 pos 5-9, Z4 pos 5-9
    if zona in [3, 4]: return '<span class="chip c-rb">REV·B</span>'
    return '<span class="chip c-ra">REV·A</span>'
 
# ═══════════════════════════════════════════════
# RENDER — ZONA 1F  (reemplaza la función existente)
# Columnas: # | Equipo | PTS | PJ | G | E | P | GOL | DG | RACHA
# RACHA se llena con JS desde PERFILES_JSON
# ═══════════════════════════════════════════════
def render_zona_1f(z, data, mejor_5to_nombre):
    color    = ZONE_COLORS.get(z, '#888')
    q        = ZONA_CONFIG[z]['qualify']
    dest_2f  = ZONA_CONFIG[z]['dest_2f']
    dest_rev = 'A' if z in [1, 2] else 'B'
 
    ZONE_RGBA = {
        1: 'rgba(41,182,246,.09)',
        2: 'rgba(102,187,106,.09)',
        3: 'rgba(255,112,67,.09)',
        4: 'rgba(242,98,146,.09)',
    }
    tint = ZONE_RGBA.get(z, 'transparent')
 
    rows = ''
    for i, t in enumerate(data):
        pos          = i + 1
        nombre       = t['nombre']
        dg           = t['gf'] - t['gc']
        dgs          = ('+' + str(dg)) if dg > 0 else ('—' if dg == 0 else str(dg))
        dgc          = 'dg-pos' if dg > 0 else ('dg-neg' if dg < 0 else 'dg-zer')
        sep          = ' class="tsep"' if pos == q + 1 else ''
        es_mejor_5to = (nombre == mejor_5to_nombre)
        clasifica    = (pos <= q) or es_mejor_5to
        bg           = f'background:{tint}' if clasifica else ''
 
        rows += (
            f'<tr{sep} data-equipo="{nombre}" style="{bg}">'
            f'<td class="td-n col-n">{pos}</td>'
            f'<td class="td-l col-eq">{nombre}</td>'
            f'<td class="td-pts col-pts" style="text-align:right">{t["pts"]}</td>'
            f'<td class="col-pj" style="text-align:right">{t["pj"]}</td>'
            f'<td class="col-gep" style="text-align:right">{t["pg"]}</td>'
            f'<td class="col-gep" style="text-align:right">{t["pe"]}</td>'
            f'<td class="col-gep" style="text-align:right">{t["pp"]}</td>'
            f'<td class="td-c col-gol">{t["gf"]}:{t["gc"]}</td>'
            f'<td class="col-dg {dgc}" style="text-align:right">{dgs}</td>'
            f'<td class="col-racha td-c"></td>'
            f'</tr>'
        )
 
    return (
        f'<div class="zcard">'
        f'<div class="zhdr">'
        f'<div class="zdot" style="background:{color}"></div>'
        f'<div class="zname" style="color:{color}">Zona {z}</div>'
        f'<div class="zmeta">{len(data)} eq · Top {q} → 2F·{dest_2f} · Resto → REV·{dest_rev}</div>'
        f'</div>'
        f'<div class="tscroll"><table>'
        f'<thead><tr>'
        f'<th class="th-c col-n">#</th>'
        f'<th class="th-l col-eq">Equipo</th>'
        f'<th style="text-align:right">PTS</th>'
        f'<th style="text-align:right">PJ</th>'
        f'<th style="text-align:right">G</th>'
        f'<th style="text-align:right">E</th>'
        f'<th style="text-align:right">P</th>'
        f'<th style="text-align:center">GOL</th>'
        f'<th style="text-align:right">DG</th>'
        f'<th class="th-c" style="text-align:center">RACHA</th>'
        f'</tr></thead>'
        f'<tbody>{rows}</tbody>'
        f'</table></div>'
        f'</div>'
    )
 
 
def render_stats_ataque(stats_equipos):
    ordenados = sorted(stats_equipos, key=lambda x: (-x['gf'], -x['gf_pj']))
    rows = ''
    for i, t in enumerate(ordenados[:12]):
        zc = ZONE_COLORS.get(t['zona'], '#888')
        rows += (f'<tr>'
                 f'<td class="n">{i+1}</td>'
                 f'<td class="l">{t["nombre"]}</td>'
                 f'<td style="color:{zc};font-size:10px;text-align:right">Z{t["zona"]}</td>'
                 f'<td style="text-align:right">{t["pj"]}</td>'
                 f'<td class="p">{t["gf"]}</td>'
                 f'<td style="color:var(--t3);text-align:right">{t["gf_pj"]:.2f}</td>'
                 f'</tr>')
    return (f'<div class="zcard">'
            f'<div class="zhdr">'
            f'<div class="zname">Ataque</div>'
            f'<div class="zmeta">GF total · GF/PJ</div>'
            f'</div>'
            f'<div class="tscroll"><table>'
            f'<thead><tr>'
            f'<th class="c">#</th><th class="l">Equipo</th>'
            f'<th style="text-align:right">Z</th>'
            f'<th style="text-align:right">PJ</th>'
            f'<th style="text-align:right">GF</th>'
            f'<th style="text-align:right">x/PJ</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody>'
            f'</table></div>'
            f'</div>')
 
def render_stats_defensa(stats_equipos):
    # Solo equipos con al menos 1 partido jugado
    con_pj = [t for t in stats_equipos if t['pj'] > 0]
    ordenados = sorted(con_pj, key=lambda x: (x['gc'], x['gc_pj']))
    rows = ''
    for i, t in enumerate(ordenados[:12]):
        zc = ZONE_COLORS.get(t['zona'], '#888')
        gc_str = str(t['gc']) if t['gc'] > 0 else '0'
        rows += (f'<tr>'
                 f'<td class="n">{i+1}</td>'
                 f'<td class="l">{t["nombre"]}</td>'
                 f'<td style="color:{zc};font-size:10px;text-align:right">Z{t["zona"]}</td>'
                 f'<td style="text-align:right">{t["pj"]}</td>'
                 f'<td style="color:var(--gp);text-align:right;font-weight:500">{gc_str}</td>'
                 f'<td style="color:var(--t3);text-align:right">{t["gc_pj"]:.2f}</td>'
                 f'</tr>')
    return (f'<div class="zcard">'
            f'<div class="zhdr">'
            f'<div class="zname">Defensa</div>'
            f'<div class="zmeta">GC total · GC/PJ ↑ mejor</div>'
            f'</div>'
            f'<div class="tscroll"><table>'
            f'<thead><tr>'
            f'<th class="c">#</th><th class="l">Equipo</th>'
            f'<th style="text-align:right">Z</th>'
            f'<th style="text-align:right">PJ</th>'
            f'<th style="text-align:right">GC</th>'
            f'<th style="text-align:right">x/PJ</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody>'
            f'</table></div>'
            f'</div>')
 
def render_vallas_invictas(stats_equipos):
    vallas = [t for t in stats_equipos if t['valla']]
    vallas.sort(key=lambda x: (-x['pj'], x['nombre']))
    if not vallas:
        contenido = '<div class="valla-list"><div class="valla-row"><span style="color:var(--t3)">Sin vallas invictas</span></div></div>'
    else:
        rows = ''
        for t in vallas:
            zc = ZONE_COLORS.get(t['zona'], '#888')
            rows += (f'<div class="valla-row">'
                     f'<span class="valla-nombre">{t["nombre"]}</span>'
                     f'<span style="display:flex;align-items:center;gap:6px">'
                     f'<span style="color:{zc};font-size:10px">Z{t["zona"]}</span>'
                     f'<span style="color:var(--t3);font-size:10px">{t["pj"]}PJ</span>'
                     f'<span class="valla-badge">▲ invicto</span>'
                     f'</span>'
                     f'</div>')
        contenido = f'<div class="valla-list">{rows}</div>'
    return (f'<div class="zcard">'
            f'<div class="zhdr">'
            f'<div class="zname">Vallas invictas</div>'
            f'<div class="zmeta">GC = 0 · sin goles recibidos</div>'
            f'</div>'
            f'{contenido}'
            f'</div>')
 
def calcular_partidos_destacados(df_carga, top_n=5):
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    jugados['GF'] = jugados['GF'].astype(int)
    jugados['GC'] = jugados['GC'].astype(int)
    jugados['total_goles'] = jugados['GF'] + jugados['GC']
    jugados = jugados.sort_values('total_goles', ascending=False).head(top_n)
    result = []
    for _, r in jugados.iterrows():
        result.append({
            'local':     str(r['Local']).strip(),
            'visitante': str(r['Visitante']).strip(),
            'gf':        int(r['GF']),
            'gc':        int(r['GC']),
            'total':     int(r['total_goles']),
            'fecha':     int(r['Fecha']) if pd.notna(r['Fecha']) else 0,
        })
    return result
 
def render_partidos_destacados(partidos):
    rows = ''
    for p in partidos:
        total_str = f'{p["total"]} goles'
        rows += (f'<div class="ptdo-row">'
                 f'<span class="ptdo-res">{p["gf"]}–{p["gc"]}</span>'
                 f'<span class="ptdo-eq"><strong>{p["local"]}</strong> vs {p["visitante"]}</span>'
                 f'<span class="ptdo-f">F{p["fecha"]} · {total_str}</span>'
                 f'</div>')
    return (f'<div class="zcard">'
            f'<div class="zhdr">'
            f'<div class="zname">Partidos más goleados</div>'
            f'<div class="zmeta">total goles en el partido</div>'
            f'</div>'
            f'{rows}'
            f'</div>')
 
# ═══════════════════════════════════════════════
# CALCULAR STATS COMPLETAS
# ═══════════════════════════════════════════════
def calcular_stats_full(df_carga, df_goles, standings_1f):
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna()].copy()
    jugados = jugados[jugados['Fase'].isin(['1F-RR2','1F'])]
    jugados['GF'] = jugados['GF'].astype(int)
    jugados['GC'] = jugados['GC'].astype(int)
 
    total = len(jugados)
 
    # ── Resultados globales
    local_g = int((jugados['GF'] > jugados['GC']).sum())
    empates  = int((jugados['GF'] == jugados['GC']).sum())
    visit_g  = int((jugados['GF'] < jugados['GC']).sum())
    cero_cero = int(((jugados['GF']==0) & (jugados['GC']==0)).sum())
 
    # ── Goles por tiempo
    df_g = df_goles.copy()
    df_g['es_ec'] = df_g['Jugador'].str.contains(r'\(e/c\)', na=False)
    goles_validos = df_g[~df_g['es_ec']].copy()
    goles_1t = int((goles_validos['Tiempo'] == '1T').sum())
    goles_2t = int((goles_validos['Tiempo'] == '2T').sum())
    penales  = int(df_g['Jugador'].str.contains(r'\(p\)', na=False).sum())
 
    # ── Tramos (minuto absoluto)
    goles_validos['min_abs'] = goles_validos.apply(
        lambda r: r['Minuto'] if r['Tiempo'] == '1T' else r['Minuto'] + 45, axis=1)
    def tramo(m):
        if m <= 15:  return '01-15'
        if m <= 30:  return '16-30'
        if m <= 45:  return '31-45'
        if m <= 60:  return '46-60'
        if m <= 75:  return '61-75'
        return '76-90'
    goles_validos['tramo'] = goles_validos['min_abs'].apply(tramo)
    tramos = goles_validos.groupby('tramo').size().reindex(
        ['01-15','16-30','31-45','46-60','61-75','76-90'], fill_value=0).to_dict()
 
    # ── Local vs Visitante por equipo
    loc = {}
    vis = {}
    zona_map = {}
    for _, r in jugados.iterrows():
        z = int(r['Zona'])
        zona_map[r['Local']] = z
        zona_map[r['Visitante']] = z
        for d, eq, gf, gc in [(loc, r['Local'], r['GF'], r['GC']),
                               (vis, r['Visitante'], r['GC'], r['GF'])]:
            if eq not in d:
                d[eq] = {'pj':0,'g':0,'e':0,'p':0,'gf':0,'gc':0,'pts':0}
            t = d[eq]
            t['pj']+=1; t['gf']+=gf; t['gc']+=gc
            if gf>gc:    t['g']+=1; t['pts']+=3
            elif gf==gc: t['e']+=1; t['pts']+=1
            else:        t['p']+=1
 
    def sort_lv(d):
        return sorted(
            [{'nombre':k, 'zona':zona_map.get(k,0), **v} for k,v in d.items()],
            key=lambda x: (-x['pts'], -(x['gf']-x['gc']), -x['gf'])
        )
 
    # ── Racha (todos los partidos en orden)
    racha_map = {}
    for _, r in jugados.sort_values('N° Partido').iterrows():
        z = int(r['Zona'])
        for nombre, gf, gc in [(r['Local'], r['GF'], r['GC']),
                                (r['Visitante'], r['GC'], r['GF'])]:
            if nombre not in racha_map:
                racha_map[nombre] = {'zona': z, 'res': [], 'pts': 0}
            res = 'G' if gf > gc else ('E' if gf == gc else 'P')
            racha_map[nombre]['res'].append(res)
            racha_map[nombre]['pts'] += 3 if res=='G' else (1 if res=='E' else 0)
 
    rachas = sorted(
        [{'nombre':k, **v} for k,v in racha_map.items()],
        key=lambda x: (-x['pts'], x['nombre'])
    )
 
    # ── Variedad de scoring (goleadores distintos por equipo)
    goles_validos['Jugador_limpio'] = (goles_validos['Jugador']
        .str.replace(r'\s*\(e/c\)\s*','',regex=True)
        .str.replace(r'\s*\(p\)\s*','',regex=True)
        .str.strip())
    variedad = (goles_validos.groupby('Equipo que convierte')['Jugador_limpio']
                .nunique().reset_index()
                .rename(columns={'Jugador_limpio':'distintos',
                                 'Equipo que convierte':'nombre'}))
    # Agregar total goles
    total_gf = (goles_validos.groupby('Equipo que convierte').size()
                .reset_index(name='total_gf')
                .rename(columns={'Equipo que convierte':'nombre'}))
    variedad = variedad.merge(total_gf, on='nombre', how='left').fillna(0)
    variedad['zona'] = variedad['nombre'].map(zona_map).fillna(0).astype(int)
    variedad = variedad.sort_values('distintos', ascending=False)
 
    # ── Partidos destacados
    jugados['total_goles'] = jugados['GF'] + jugados['GC']
    top_partidos = jugados.nlargest(5, 'total_goles')[
        ['Local','Visitante','GF','GC','total_goles','Fecha']].to_dict('records')
 
    return {
        'total':      total,
        'local_g':    local_g,
        'empates':    empates,
        'visit_g':    visit_g,
        'cero_cero':  cero_cero,
        'goles_1t':   goles_1t,
        'goles_2t':   goles_2t,
        'penales':    penales,
        'tramos':     tramos,
        'mejor_local':    sort_lv(loc),
        'mejor_visitante': sort_lv(vis),
        'rachas':     rachas,
        'variedad':   variedad.to_dict('records'),
        'top_partidos': top_partidos,
        'stats_equipos': calcular_stats_equipos(df_carga, standings_1f),
    }
 
# ═══════════════════════════════════════════════
# RENDER — STATS FULL
# ═══════════════════════════════════════════════
 
# ═══════════════════════════════════════════════
# RENDER — STATS FULL
# ═══════════════════════════════════════════════
 
# ═══════════════════════════════════════════════
# GOLES POR TRAMO POR EQUIPO
# ═══════════════════════════════════════════════
def calcular_goles_tramo_equipos(df_carga, df_goles, standings_1f):
    from collections import defaultdict
    df_g = df_goles.copy()
    df_g['Tiempo']     = df_g['Tiempo'].astype(str).str.strip()
    df_g['Minuto']     = pd.to_numeric(df_g['Minuto'], errors='coerce')
    df_g['N° Partido'] = pd.to_numeric(df_g['N° Partido'], errors='coerce')
    df_g['min_abs']    = df_g.apply(
        lambda r: r['Minuto'] if r['Tiempo']=='1T' else r['Minuto']+45, axis=1)
 
    TRAMOS = [('1-15',1,15),('16-30',16,30),('31-45',31,45),
              ('46-60',46,60),('61-75',61,75),('76-90',76,95)]
 
    partidos = {}
    for _, r in df_carga.iterrows():
        if pd.notna(r.get('N° Partido')):
            partidos[int(r['N° Partido'])] = {
                'local':     str(r['Local']).strip(),
                'visitante': str(r['Visitante']).strip(),
                'fase':      str(r.get('Fase','')).strip(),
            }
 
    zona_map = {}
    pos_map  = {}
    for zona, equipos in standings_1f.items():
        for i, t in enumerate(equipos):
            zona_map[t['nombre']] = int(zona)
            pos_map[t['nombre']]  = i + 1
 
    stats = defaultdict(lambda: {
        'tramos': {t[0]: {'m':0,'r':0,'ml':0,'rl':0,'mv':0,'rv':0} for t in TRAMOS},
        'zona': 0, 'pos': 0, 'fase': '1F-RR2'
    })
 
    for _, g in df_g.iterrows():
        nro = int(g['N° Partido']) if pd.notna(g['N° Partido']) else None
        if nro not in partidos: continue
        p    = partidos[nro]
        conv = str(g['Equipo que convierte']).strip()
        loc, vis = p['local'], p['visitante']
        min_abs  = g['min_abs']
        if pd.isna(min_abs): continue
        es_local = (conv == loc)
        rival    = vis if es_local else loc
 
        for label, a, b in TRAMOS:
            if a <= min_abs <= b:
                for eq in [conv, rival]:
                    stats[eq]['zona'] = zona_map.get(eq, 0)
                    stats[eq]['pos']  = pos_map.get(eq, 0)
                    stats[eq]['fase'] = p['fase']
                tc = stats[conv]['tramos'][label]
                tr = stats[rival]['tramos'][label]
                tc['m'] += 1; tr['r'] += 1
                if es_local:
                    tc['ml'] += 1; tr['rv'] += 1
                else:
                    tc['mv'] += 1; tr['rl'] += 1
                break
 
    result = []
    TRAMOS_LABELS = [t[0] for t in TRAMOS]
    for nombre, d in stats.items():
        row = {'nombre': nombre, 'zona': d['zona'], 'pos': d['pos'], 'fase': d['fase']}
        for label in TRAMOS_LABELS:
            t = d['tramos'][label]
            row[f'{label}_m']  = t['m'];  row[f'{label}_r']  = t['r']
            row[f'{label}_ml'] = t['ml']; row[f'{label}_rl'] = t['rl']
            row[f'{label}_mv'] = t['mv']; row[f'{label}_rv'] = t['rv']
        result.append(row)
    return result
 
 
def render_goles_tramo(data):
    import json as _json
    filas  = data.get('goles_por_tramo_eq', [])
    fases  = sorted({t.get('fase','1F-RR2') for t in filas})
    multi  = len(fases) > 1
    fj     = _json.dumps(filas, ensure_ascii=False, default=str)
    zc_js  = _json.dumps({1:'#0288d1',2:'#2e7d32',3:'#e65100',4:'#c2185b'})
    TRAMOS = ['1-15','16-30','31-45','46-60','61-75','76-90']
    tj     = _json.dumps(TRAMOS)
 
    filtros_fase = ''
    if multi:
        btns = '<button class="tbtn active" data-grupo="gfase" data-val="TODAS">TODAS</button>'
        btns += ''.join(f'<button class="tbtn" data-grupo="gfase" data-val="{f}">{f}</button>'
                        for f in fases)
        filtros_fase = f'<div style="display:flex;gap:4px;flex-wrap:wrap">{btns}</div>'
 
    filtros_cond = ('<div style="display:flex;gap:4px;flex-wrap:wrap">'
                    '<button class="tbtn active" data-grupo="gcond" data-val="GLOBAL">GLOBAL</button>'
                    '<button class="tbtn" data-grupo="gcond" data-val="LOCAL">LOCAL</button>'
                    '<button class="tbtn" data-grupo="gcond" data-val="VISITANTE">VISITANTE</button>'
                    '</div>')
    sep = '<div style="width:1px;height:20px;background:var(--s3)"></div>' if multi else ''
    filtros_wrap = (f'<div style="display:flex;gap:12px;align-items:center;'
                    f'margin-bottom:10px;flex-wrap:wrap">{filtros_fase}{sep}{filtros_cond}</div>')
 
    BD = 'border-bottom:1px solid var(--s3)'
    th_s = (f'style="position:sticky;top:0;background:var(--s2);padding:4px 8px;'
            f'text-align:right;color:var(--t3);font-weight:400;font-size:10px;'
            f'white-space:nowrap;{BD};z-index:1"')
 
    thead = ('<thead><tr>'
             f'<th style="position:sticky;left:0;top:0;background:var(--s2);'
             f'padding:4px 12px;text-align:left;color:var(--t3);font-weight:400;'
             f'font-size:10px;white-space:nowrap;{BD};border-right:1px solid var(--s3);z-index:3">EQUIPO</th>'
             f'<th {th_s}>Z</th><th {th_s}>POS</th>')
    for t in TRAMOS:
        thead += (f'<th colspan="2" style="position:sticky;top:0;background:var(--s2);'
                  f'padding:4px 8px;text-align:center;color:var(--t3);font-weight:500;'
                  f'font-size:10px;white-space:nowrap;{BD};border-left:1px solid var(--s3);z-index:1">'
                  f'{t}</th>')
    thead += ('</tr><tr>'
              f'<th style="position:sticky;left:0;top:21px;background:var(--s2);{BD};'
              f'border-right:1px solid var(--s3);z-index:3"></th>'
              f'<th style="position:sticky;top:21px;background:var(--s2);{BD};z-index:1"></th>'
              f'<th style="position:sticky;top:21px;background:var(--s2);{BD};z-index:1"></th>')
    for _ in TRAMOS:
        thead += (f'<th style="position:sticky;top:21px;background:var(--s2);padding:3px 8px;'
                  f'text-align:right;color:var(--gp);font-size:9px;font-weight:500;{BD};'
                  f'border-left:1px solid var(--s3);z-index:1">M</th>'
                  f'<th style="position:sticky;top:21px;background:var(--s2);padding:3px 8px;'
                  f'text-align:right;color:var(--gn);font-size:9px;font-weight:500;{BD};z-index:1">R</th>')
    thead += '</tr></thead>'
 
    return f'''{filtros_wrap}
<div style="max-height:520px;overflow:auto;border:1px solid var(--s3);border-radius:4px">
  <table id="tabla-tramos" style="font-size:11px;width:100%;border-collapse:collapse">
    {thead}
    <tbody id="tt-body"></tbody>
  </table>
</div>
<div style="font-size:10px;color:var(--t3);margin-top:6px">
  M=marcados · R=recibidos · tramos en minutos absolutos (2T suma 45 al minuto relativo)
</div>
<script>
(function(){{
  const FILAS={fj};
  const ZC={zc_js};
  const TRAMOS={tj};
  let faseG='TODAS', condG='GLOBAL';
 
  document.querySelectorAll('.tbtn[data-grupo^="g"]').forEach(function(btn){{
    btn.addEventListener('click', function(){{
      const g=this.dataset.grupo;
      document.querySelectorAll('.tbtn[data-grupo="'+g+'"]').forEach(function(b){{b.classList.remove('active');}});
      this.classList.add('active');
      if(g==='gfase') faseG=this.dataset.val;
      else condG=this.dataset.val;
      render();
    }});
  }});
 
  function render(){{
    let filas=faseG==='TODAS'?FILAS:FILAS.filter(function(t){{return t.fase===faseG;}});
    filas=[...filas].sort(function(a,b){{
      return a.zona!==b.zona?a.zona-b.zona:a.pos-b.pos;
    }});
    const suf=condG==='LOCAL'?'l':condG==='VISITANTE'?'v':'';
    const BD='border-bottom:1px solid var(--s3);white-space:nowrap';
    document.getElementById('tt-body').innerHTML=filas.map(function(t){{
      const zc=ZC[t.zona]||'#888';
      let row='<tr>'
        +'<td style="position:sticky;left:0;background:var(--bg);padding:3px 12px;'+BD
        +';border-right:1px solid var(--s3);color:var(--t1);min-width:140px">'+t.nombre+'</td>'
        +'<td style="padding:3px 8px;text-align:right;'+BD+';color:'+zc+';font-size:10px">'+t.zona+'</td>'
        +'<td style="padding:3px 8px;text-align:right;'+BD+';color:var(--t2)">'+t.pos+'</td>';
      TRAMOS.forEach(function(tr){{
        const mk=tr+'_m'+suf, rk=tr+'_r'+suf;
        const m=t[mk]||0, r=t[rk]||0;
        row+='<td style="padding:3px 8px;text-align:right;'+BD+';border-left:1px solid var(--s3);'
          +'color:var(--gp);font-weight:'+(m>0?'500':'400')+'">'+(m||'-')+'</td>';
        row+='<td style="padding:3px 8px;text-align:right;'+BD+';color:var(--gn);'
          +'font-weight:'+(r>0?'500':'400')+'">'+(r||'-')+'</td>';
      }});
      row+='</tr>';
      return row;
    }}).join('');
  }}
 
  render();
}})();
</script>'''
 
 
def render_proy_full(data):
    """Renderiza proyección completa: 2F (Zona A y B) + Reválida (Zona A y B)."""
 
    zona_a_2f = data.get('zona_a_2f', [])
    zona_b_2f = data.get('zona_b_2f', [])
    rev_a     = data.get('rev_a_1e', [])
    rev_b     = data.get('rev_b_1e', [])
    mejor_5to = data.get('mejor_5to')
    mejor_5to_nombre = mejor_5to['nombre'] if mejor_5to else ''
 
    def subtitulo(txt, sub=''):
        s = f' <span style="color:var(--t3);font-size:10px;font-weight:400;letter-spacing:0">{sub}</span>' if sub else ''
        return (f'<div style="font-family:var(--grotesk);font-size:10px;font-weight:500;'
                f'letter-spacing:.12em;color:var(--t3);text-transform:uppercase;'
                f'margin:20px 0 8px;padding-bottom:5px;border-bottom:1px solid var(--s3)">'
                f'{txt}{s}</div>')
 
    def tabla_2f(label, color, equipos, corte=4):
        rows = ''
        for i, t in enumerate(equipos):
            zc   = ZONE_COLORS.get(t.get('zona', 1), '#888')
            sep  = ' style="border-top:1px dashed var(--s3)"' if i == corte else ''
            dest = ('<span class="chip c-3f">→ 3F</span>' if i < corte
                    else '<span class="chip c-r">→ REV</span>')
            es5  = (t.get('nombre') == mejor_5to_nombre)
            src_color = f'color:{zc}' if zc else ''
            rows += (f'<tr{sep}>'
                     f'<td class="n">{i+1}</td>'
                     f'<td class="l">{t["nombre"]}{"&nbsp;★" if es5 else ""}</td>'
                     f'<td style="font-size:10px;text-align:right;{src_color}">{t.get("src","")}</td>'
                     f'<td style="text-align:right">{t.get("pts","")}</td>'
                     f'<td class="c">{dest}</td>'
                     f'</tr>')
        return (f'<div class="zcard">'
                f'<div class="zhdr">'
                f'<div class="zdot" style="background:{color}"></div>'
                f'<div class="zname" style="color:{color}">Zona {label}</div>'
                f'<div class="zmeta">Top 4 → 3F</div>'
                f'</div>'
                f'<div class="tscroll"><table>'
                f'<thead><tr><th class="c">#</th><th class="l">Equipo</th>'
                f'<th style="text-align:right">Origen</th>'
                f'<th style="text-align:right">PTS</th>'
                f'<th class="c">→</th></tr></thead>'
                f'<tbody>{rows}</tbody></table></div></div>')
 
    def tabla_rev(label, equipos, criterio):
        rows = ''
        for i, t in enumerate(equipos):
            zc  = ZONE_COLORS.get(t.get('zona', 1), '#888')
            sep = ' style="border-top:1px dashed var(--s3)"' if i == 5 else ''
            dest = ('<span class="chip c-a" style="font-size:9px">→ 2E</span>' if i < 5
                    else '<span style="font-size:9px;color:var(--dn)">↓ DESC</span>')
            rows += (f'<tr{sep}>'
                     f'<td class="n">{i+1}</td>'
                     f'<td class="l">{t["nombre"]}</td>'
                     f'<td style="font-size:10px;text-align:right;color:{zc}">{t.get("src","")}</td>'
                     f'<td style="text-align:right">{t.get("pts","")}</td>'
                     f'<td class="c">{dest}</td>'
                     f'</tr>')
        return (f'<div class="zcard">'
                f'<div class="zhdr">'
                f'<div class="zdot" style="background:var(--rev)"></div>'
                f'<div class="zname" style="color:var(--rev)">Zona {label}</div>'
                f'<div class="zmeta">{criterio}</div>'
                f'</div>'
                f'<div class="tscroll"><table>'
                f'<thead><tr><th class="c">#</th><th class="l">Equipo</th>'
                f'<th style="text-align:right">Origen</th>'
                f'<th style="text-align:right">PTS</th>'
                f'<th class="c">→</th></tr></thead>'
                f'<tbody>{rows}</tbody></table></div></div>')
 
    html  = subtitulo('SEGUNDA FASE', f'18 clasificados · {len(zona_a_2f)+len(zona_b_2f)} proyectados')
    html += f'<div class="g2">{tabla_2f("A","var(--z1)",zona_a_2f)}{tabla_2f("B","var(--z3)",zona_b_2f)}</div>'
 
    html += subtitulo('REVÁLIDA — 1RA ETAPA', f'19 equipos · top 5 c/zona → 2da etapa · 2 peores descienden')
    html += f'<div class="g2">{tabla_rev("A", rev_a, "prom. puntos · ↓ 2 peores")}{tabla_rev("B", rev_b, "puntos totales · ↓ 2 peores")}</div>'
 
    nota = (f'<div style="font-size:10px;color:var(--t3);margin-top:8px">'
            f'★ = mejor 5to de Z2/Z3/Z4 &nbsp;·&nbsp; '
            f'2E = clasifica a 2da Etapa Reválida &nbsp;·&nbsp; '
            f'DESC = desciende al Regional 2027 &nbsp;·&nbsp; '
            f'Corte punteado = límite de clasificación'
            f'</div>')
    html += nota
    return html
 
def render_goleadores(goleadores, fecha_actual):
    mitad = 10
    col_a = goleadores[:mitad]
    col_b = goleadores[mitad:mitad*2]
 
    def tabla_col(items, offset=0):
        rows = ''
        for i, g in enumerate(items):
            nota = (f' <span style="font-size:9px;color:var(--t3)">({g["penales"]}p)</span>'
                    if g['penales'] > 0 else '')
            rows += (f'<tr>'
                     f'<td class="n">{i+1+offset}</td>'
                     f'<td class="l">{g["jugador"]}{nota}</td>'
                     f'<td style="color:var(--t2);font-size:10px">{g["equipo"]}</td>'
                     f'<td class="p">{g["goles"]}</td>'
                     f'</tr>')
        return (f'<div class="zcard">'
                f'<div class="zhdr"><div class="zname">Goleadores</div>'
                f'<div class="zmeta">F1–F{fecha_actual}</div></div>'
                f'<div class="tscroll"><table>'
                f'<thead><tr><th class="c">#</th><th class="l">Jugador</th>'
                f'<th class="l">Equipo</th><th style="text-align:right">G</th>'
                f'</tr></thead><tbody>{rows}</tbody></table></div></div>')
 
    html = tabla_col(col_a, 0)
    if col_b:
        html += tabla_col(col_b, mitad)
    return html
 
# ═══════════════════════════════════════════════
# RENDER — STATS FULL (tablas duras)
# ═══════════════════════════════════════════════
def calcular_stats_duras(df_carga, df_goles):
    """Calcula globales, por fecha y por zona para la solapa Estadísticas."""
    jugados = df_carga[df_carga['GF'].notna() & df_carga['GC'].notna() &
                       df_carga['Fase'].isin(['1F-RR2','1F'])].copy()
    jugados['GF']    = jugados['GF'].astype(int)
    jugados['GC']    = jugados['GC'].astype(int)
    jugados['Fecha'] = jugados['Fecha'].astype(int)
    jugados['Zona']  = jugados['Zona'].astype(int)
    jugados['tg']    = jugados['GF'] + jugados['GC']
 
    df_g = df_goles.copy()
    df_g['Tiempo']  = df_g['Tiempo'].astype(str).str.strip()
 
    total_p = len(jugados)
    total_g = int(jugados['tg'].sum())
    loc_g   = int((jugados['GF'] > jugados['GC']).sum())
    emp     = int((jugados['GF'] == jugados['GC']).sum())
    vis_g   = int((jugados['GF'] < jugados['GC']).sum())
    goles_l = int(jugados['GF'].sum())
    goles_v = int(jugados['GC'].sum())
    t1      = int((df_g['Tiempo'] == '1T').sum())
    t2      = int((df_g['Tiempo'] == '2T').sum())
    penales = int(df_g['Jugador'].str.contains(r'\(p\)', na=False).sum())
    en_cont = int(df_g['Jugador'].str.contains(r'\(e/c\)', na=False).sum())
    cero    = int((jugados['tg'] == 0).sum())
    max_g   = int(jugados['tg'].max()) if total_p else 0
    prom    = round(total_g / total_p, 2) if total_p else 0
 
    globales = [
        ('Partidos jugados',          total_p,                   ''),
        ('Goles totales',             total_g,                   ''),
        ('Promedio goles/partido',    prom,                      ''),
        ('Goles en 1er tiempo',       t1,                        f'{round(t1/total_g*100) if total_g else 0}%'),
        ('Goles en 2do tiempo',       t2,                        f'{round(t2/total_g*100) if total_g else 0}%'),
        ('Goles del local',           goles_l,                   f'{round(goles_l/total_g*100) if total_g else 0}%'),
        ('Goles del visitante',       goles_v,                   f'{round(goles_v/total_g*100) if total_g else 0}%'),
        ('Local gana',                loc_g,                     f'{round(loc_g/total_p*100) if total_p else 0}%'),
        ('Empate',                    emp,                       f'{round(emp/total_p*100) if total_p else 0}%'),
        ('Visitante gana',            vis_g,                     f'{round(vis_g/total_p*100) if total_p else 0}%'),
        ('Partidos sin goles (0-0)',  cero,                      f'{round(cero/total_p*100) if total_p else 0}%'),
        ('Goles de penal',            penales,                   f'{round(penales/total_g*100) if total_g else 0}%'),
        ('Goles en contra',           en_cont,                   ''),
        ('Partido con más goles',     max_g,                     'goles en el partido'),
    ]
 
    # Por fecha
    por_fecha = []
    for fecha in sorted(jugados['Fecha'].unique()):
        sub = jugados[jugados['Fecha'] == fecha]
        tg_f = int(sub['tg'].sum())
        pj_f = len(sub)
        por_fecha.append({
            'fecha':    int(fecha),
            'partidos': pj_f,
            'goles':    tg_f,
            'prom':     round(tg_f / pj_f, 2) if pj_f else 0,
            'loc_g':    int((sub['GF'] > sub['GC']).sum()),
            'emp':      int((sub['GF'] == sub['GC']).sum()),
            'vis_g':    int((sub['GF'] < sub['GC']).sum()),
            'goles_loc':int(sub['GF'].sum()),
            'goles_vis':int(sub['GC'].sum()),
            'max_g':    int(sub['tg'].max()),
        })
 
    # Por zona
    por_zona = []
    for zona in sorted(jugados['Zona'].unique()):
        sub = jugados[jugados['Zona'] == zona]
        tg_z = int(sub['tg'].sum())
        pj_z = len(sub)
        por_zona.append({
            'zona':     int(zona),
            'partidos': pj_z,
            'goles':    tg_z,
            'prom':     round(tg_z / pj_z, 2) if pj_z else 0,
            'loc_g':    int((sub['GF'] > sub['GC']).sum()),
            'emp':      int((sub['GF'] == sub['GC']).sum()),
            'vis_g':    int((sub['GF'] < sub['GC']).sum()),
            'goles_loc':int(sub['GF'].sum()),
            'goles_vis':int(sub['GC'].sum()),
        })
 
    return {'globales': globales, 'por_fecha': por_fecha, 'por_zona': por_zona}
 
 
 
# ═══════════════════════════════════════════════
# RENDER — STATS FULL  (reemplaza la función existente)
# Columnas: EQUIPO | Z | PTS/PJ | %RDT | GF/PJ | GC/PJ | %G | %E | %P
# ═══════════════════════════════════════════════
def render_stats_full(data, fa):
    import json as _json
 
    se         = data.get('stats_equipos', [])
    filas_json = _json.dumps(se, ensure_ascii=False, default=str)
    zcolors_js = _json.dumps({1: '#29b6f6', 2: '#66bb6a', 3: '#ff7043', 4: '#f06292'})
 
    TODAS_FASES   = ['1F', '2F', 'REV-1']
    fases_disp    = sorted({t.get('fase_actual', '1F') for t in se})
    fases_activas = set(fases_disp)
 
    # FASE select
    options = '<option value="TODAS">TODAS LAS FASES</option>'
    for f in TODAS_FASES:
        if f in fases_activas:
            options += f'<option value="{f}">{f}</option>'
        else:
            options += f'<option value="{f}" disabled>{f} —</option>'
 
    select_fase = (
        f'<select id="sel-fase" style="font-family:var(--grotesk);font-size:10px;'
        f'padding:3px 8px;border:1px solid var(--s3);border-radius:2px;'
        f'background:var(--s1);color:var(--t1);cursor:pointer;letter-spacing:.04em">'
        f'{options}</select>'
    )
 
    btn_cond = (
        '<div style="display:flex;gap:4px">'
        '<button class="tbtn active" data-g="cond" data-v="GLOBAL">GLOBAL</button>'
        '<button class="tbtn" data-g="cond" data-v="LOCAL">LOCAL</button>'
        '<button class="tbtn" data-g="cond" data-v="VISITANTE">VISITANTE</button>'
        '</div>'
    )
 
    h2h_disabled = '' if fa >= 3 else ' disabled'
    h2h_hint_txt = '' if fa >= 3 else '<span style="font-size:10px;color:var(--t3)">disponible desde F3</span>'
    btn_h2h      = (
        f'<button id="btn-h2h" class="tbtn{h2h_disabled}" '
        f'style="letter-spacing:.06em">H2H 2026</button>'
        f'<span id="h2h-hint" style="display:none;font-size:10px;color:var(--acc)"></span>'
    )
 
    filtros = (
        f'<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:10px">'
        f'{select_fase}'
        f'<div style="width:1px;height:20px;background:var(--s3)"></div>'
        f'{btn_cond}'
        f'<div style="width:1px;height:20px;background:var(--s3)"></div>'
        f'{btn_h2h}{h2h_hint_txt}'
        f'</div>'
    )
 
    BD   = 'border-bottom:1px solid var(--s3)'
    stk  = f'position:sticky;top:0;background:var(--s2);{BD};z-index:1'
    stk3 = f'position:sticky;left:0;top:0;background:var(--s2);{BD};border-right:1px solid var(--s3);z-index:3'
 
    def th(col, label, asc='false'):
        return (
            f'<th data-col="{col}" data-asc="{asc}" '
            f'style="{stk};padding:5px 10px;text-align:right;color:var(--t3);'
            f'font-family:var(--grotesk);font-size:10px;font-weight:400;letter-spacing:.06em;'
            f'white-space:nowrap;cursor:pointer;user-select:none">'
            f'{label} <span class="si">↕</span></th>'
        )
 
    thead = (
        '<thead><tr>'
        f'<th data-col="nombre" data-asc="true" style="{stk3};padding:5px 12px;text-align:left;'
        f'color:var(--t3);font-family:var(--grotesk);font-size:10px;font-weight:400;'
        f'letter-spacing:.06em;white-space:nowrap;cursor:pointer;user-select:none;min-width:140px">'
        f'EQUIPO <span class="si">↕</span></th>'
        + th('zona',    'Z',      'true')
        + th('pts_pj',  'PTS/PJ', 'false')
        + th('rdt',     '%RDT',   'false')
        + th('gf_pj',   'GF/PJ',  'false')
        + th('gc_pj',   'GC/PJ',  'true')
        + th('pct_g',   '%G',     'false')
        + th('pct_e',   '%E',     'false')
        + th('pct_p',   '%P',     'true')
        + '</tr></thead>'
    )
 
    tabla = (
        f'<div id="stats-scroll" style="overflow:auto;-webkit-overflow-scrolling:touch;'
        f'overscroll-behavior:contain;border:1px solid var(--s3);border-radius:3px">'
        f'<table id="tabla-gral" style="font-size:11px;width:100%;border-collapse:collapse">'
        f'{thead}'
        f'<tbody id="tg-body"></tbody>'
        f'</table></div>'
        f'<style>'
        f'@media (max-width:600px) {{ #stats-scroll {{ max-height:224px }} }}'
        f'</style>'
        f'<div style="font-size:10px;color:var(--t3);margin-top:6px">'
        f'LOCAL/VISITANTE recalcula todas las métricas · Click columna para ordenar'
        f'</div>'
    )
 
    js = f'''<script>
(function(){{
  var FILAS={filas_json};
  var ZC={zcolors_js};
  var sortCol='pts_pj', sortAsc=false;
  var faseA='TODAS', condA='GLOBAL';
  var h2hMode=false, h2hSel=[], h2hAsig=false;
 
  document.getElementById('sel-fase').addEventListener('change', function(){{
    faseA=this.value; render();
  }});
 
  document.querySelectorAll('.tbtn[data-g="cond"]').forEach(function(b){{
    b.addEventListener('click', function(){{
      document.querySelectorAll('.tbtn[data-g="cond"]').forEach(function(x){{x.classList.remove('active');}});
      this.classList.add('active');
      condA=this.dataset.v; render();
    }});
  }});
 
  function metricas(t, cond){{
    var pj, pts, gf, gc;
    if(cond==='LOCAL')      {{ pj=t.loc_pj; pts=t.loc_pts; gf=t.loc_gf; gc=t.loc_gc; }}
    else if(cond==='VISITANTE') {{ pj=t.vis_pj; pts=t.vis_pts; gf=t.vis_gf; gc=t.vis_gc; }}
    else {{ pj=t.pj; pts=t.pts; gf=t.gf; gc=t.gc; }}
    if(!pj) return null;
    return {{
      pts_pj: (pts/pj).toFixed(2),
      rdt:    Math.round(pts/(pj*3)*100)+'%',
      gf_pj:  (gf/pj).toFixed(2),
      gc_pj:  (gc/pj).toFixed(2),
      pct_g: cond==='LOCAL'?(t.loc_pct_g!=null?t.loc_pct_g+'%':'-'):
             cond==='VISITANTE'?(t.vis_pct_g!=null?t.vis_pct_g+'%':'-'):
             t.pct_g+'%',
      pct_e: cond==='LOCAL'?(t.loc_pct_e!=null?t.loc_pct_e+'%':'-'):
             cond==='VISITANTE'?(t.vis_pct_e!=null?t.vis_pct_e+'%':'-'):
             t.pct_e+'%',
      pct_p: cond==='LOCAL'?(t.loc_pct_p!=null?t.loc_pct_p+'%':'-'):
             cond==='VISITANTE'?(t.vis_pct_p!=null?t.vis_pct_p+'%':'-'):
             t.pct_p+'%',
      _pts_pj_num: pj?pts/pj:0,
      _gf_pj_num:  pj?gf/pj:0,
      _gc_pj_num:  pj?gc/pj:0,
      _pct_g_num:  pj?t.pg/pj:0,
      _pct_p_num:  pj?t.pp/pj:0,
    }};
  }}
 
  function getVal(t, col){{
    if(t._m && t._m[col]!==undefined){{
      var v=t._m[col]; var p=parseFloat(v);
      return !isNaN(p)?p:String(v).toLowerCase();
    }}
    if(t[col]!==undefined){{ var p2=parseFloat(t[col]); return !isNaN(p2)?p2:String(t[col]).toLowerCase(); }}
    return 0;
  }}
 
  var SECONDARY={{
    'pts_pj':[['_gf_pj_num',false],['_gc_pj_num',true]],
    'rdt':   [['_pts_pj_num',false],['_gf_pj_num',false]],
    'gf_pj': [['_pts_pj_num',false]],
    'gc_pj': [['_pts_pj_num',false]],
    'pct_g': [['_pts_pj_num',false]],
    'pct_e': [['_pts_pj_num',false]],
    'pct_p': [['_pts_pj_num',false]],
    'zona':  [['_pts_pj_num',false]],
    'nombre':[],
  }};
 
  function render(){{
    var filas=faseA==='TODAS'?FILAS:FILAS.filter(function(t){{return t.fase_actual===faseA;}});
    if(h2hMode&&h2hSel.length===2) filas=filas.filter(function(t){{return h2hSel.indexOf(t.nombre)>=0;}});
    var withM=filas.map(function(t){{
      var cond=condA;
      if(h2hAsig&&h2hSel.length===2) cond=(t.nombre===h2hSel[0])?'LOCAL':'VISITANTE';
      return Object.assign({{}},t,{{_m:metricas(t,cond),_cond:cond}});
    }}).filter(function(t){{return t._m;}});
 
    if(h2hAsig&&h2hSel.length===2){{
      withM.sort(function(a,b){{return h2hSel.indexOf(a.nombre)-h2hSel.indexOf(b.nombre);}});
    }} else {{
      withM.sort(function(a,b){{
        var va=getVal(a,sortCol), vb=getVal(b,sortCol);
        if(va!==vb) return sortAsc?(va>vb?1:-1):(va<vb?1:-1);
        var secs=SECONDARY[sortCol]||[];
        for(var i=0;i<secs.length;i++){{
          var sc=secs[i][0]; var va2=getVal(a,sc), vb2=getVal(b,sc);
          if(va2!==vb2) return va2>vb2?-1:1;
        }}
        return 0;
      }});
    }}
 
    var BD='border-bottom:1px solid var(--s3);white-space:nowrap';
    var stk='position:sticky;left:0;z-index:2;background:var(--bg)';
    document.getElementById('tg-body').innerHTML=withM.map(function(t){{
      var m=t._m;
      var zc=ZC[t.zona]||'#888';
      var h2hc=(h2hMode?' h2h-pick':'')+(h2hMode&&h2hSel.indexOf(t.nombre)>=0?' h2h-sel':'');
      var lbl=h2hAsig&&h2hSel.length===2?'<span style="font-size:9px;color:var(--t3);margin-left:5px">'+
              (t._cond==='LOCAL'?'LOC':'VIS')+'</span>':'';
      var td=function(v){{return '<td style="padding:3px 10px;text-align:right;'+BD+';color:var(--t2)">'+v+'</td>';}};
      return '<tr class="tg-row'+h2hc+'" data-h2h="'+t.nombre.replace(/"/g,'&quot;')+'">'
        +'<td style="'+stk+';padding:3px 12px;text-align:left;'+BD+';border-right:1px solid var(--s3);color:var(--t1);min-width:140px;font-size:11px">'+t.nombre+lbl+'</td>'
        +'<td style="padding:3px 8px;text-align:right;'+BD+';color:'+zc+';font-size:10px">'+t.zona+'</td>'
        +td(m.pts_pj)+td(m.rdt)+td(m.gf_pj)+td(m.gc_pj)
        +td(m.pct_g)+td(m.pct_e)+td(m.pct_p)
        +'</tr>';
    }}).join('');
  }}
 
  // Sort on header click
  document.getElementById('tabla-gral').querySelector('thead').addEventListener('click',function(e){{
    var th=e.target.closest('th[data-col]'); if(!th) return;
    var col=th.dataset.col;
    if(col===sortCol) sortAsc=!sortAsc; else {{ sortCol=col; sortAsc=th.dataset.asc==='true'; }}
    document.querySelectorAll('#tabla-gral thead .si').forEach(function(s){{s.textContent='↕';}});
    th.querySelector('.si').textContent=sortAsc?'↑':'↓';
    render();
  }});
 
  // H2H
  document.getElementById('tg-body').addEventListener('click',function(e){{
    if(!h2hMode) return;
    var tr=e.target.closest('tr[data-h2h]'); if(!tr) return;
    var n=tr.dataset.h2h;
    var idx=h2hSel.indexOf(n);
    if(idx>=0) h2hSel.splice(idx,1);
    else if(h2hSel.length<2) h2hSel.push(n);
    h2hAsig=(h2hSel.length===2);
    var hint=document.getElementById('h2h-hint');
    if(!h2hSel.length) hint.textContent='Elegí dos equipos';
    else if(h2hSel.length===1) hint.textContent=h2hSel[0]+' vs …';
    else hint.textContent=h2hSel[0]+' vs '+h2hSel[1];
    render();
  }});
 
  var btnH2h=document.getElementById('btn-h2h');
  if(btnH2h) btnH2h.addEventListener('click',function(){{
    h2hMode=!h2hMode; h2hSel=[]; h2hAsig=false;
    this.style.background  =h2hMode?'var(--acc)':'';
    this.style.color       =h2hMode?'#fff':'';
    this.style.borderColor =h2hMode?'var(--acc)':'';
    document.querySelectorAll('.tbtn[data-g="cond"]').forEach(function(b){{
      h2hMode?b.classList.add('disabled'):b.classList.remove('disabled');
    }});
    var hint=document.getElementById('h2h-hint');
    hint.style.display=h2hMode?'inline':'none';
    hint.textContent='Elegí dos equipos';
    render();
  }});
 
  render();
}})();
</script>'''
 
    return filtros + tabla + js
 
 
def render_zona_2f(label, color, sub, data):
    rows = ''
    for i, t in enumerate(data):
        sep   = ' class="tsep"' if i == 4 else ''
        chip  = '<span class="chip c-3f">3F</span>' if i < 4 else '<span class="chip c-r">REV</span>'
        zcolor = ZONE_COLORS.get(t.get('zona', 1), '#888')
        rows += (f'<tr{sep}>'
                 f'<td class="n">{i+1}</td>'
                 f'<td class="l">{t["nombre"]}</td>'
                 f'<td style="color:{zcolor};font-size:10px;text-align:right">{t.get("src","")}</td>'
                 f'<td class="c">{chip}</td>'
                 f'</tr>')
    return (f'<div class="zcard">'
            f'<div class="zhdr">'
            f'<div class="zdot" style="background:{color}"></div>'
            f'<div class="zname" style="color:{color}">Zona {label}</div>'
            f'<div class="zmeta">{sub} · Top 4 → 3F</div>'
            f'</div>'
            f'<div class="tscroll"><table>'
            f'<thead><tr>'
            f'<th class="c">#</th><th class="l">Equipo</th>'
            f'<th style="text-align:right">Origen</th>'
            f'<th class="c">→</th>'
            f'</tr></thead>'
            f'<tbody>{rows}</tbody>'
            f'</table></div>'
            f'</div>')
 
def generar_html(data, template_path, output_path):
    with open(template_path, 'r', encoding='utf-8') as f:
        html = f.read()
 
    stats       = data['stats']
    standings   = {int(k): v for k, v in data['standings_1f'].items()}
    mejor_5to   = data.get('mejor_5to')
    mejor_5to_nombre = mejor_5to['nombre'] if mejor_5to else ''
 
    fa  = stats['fecha_actual']
    ft  = stats['fecha_total']
    pp  = stats['partidos_jugados']
    tg  = stats['total_goles']
    pct = round(fa / ft * 100) if ft else 0
 
    # Placeholders básicos
    html = html.replace('{{FASE_LABEL}}',       'PRIMERA FASE')
    html = html.replace('{{FECHA_ACTUAL}}',     str(fa))
    html = html.replace('{{FECHA_TOTAL}}',      str(ft))
    html = html.replace('{{PARTIDOS_JUGADOS}}', str(pp))
    html = html.replace('{{TOTAL_GOLES}}',      str(tg))
    html = html.replace('{{PCT_FASE}}',         str(pct))
    html = html.replace('{{META_1F}}',
        f'F{fa}/{ft} · {pp} PARTIDOS · {tg} GOLES · {pct}% DE LA FASE')
 
    # Zonas 1F
    zonas_html = ''.join(render_zona_1f(z, standings.get(z, []), mejor_5to_nombre)
                         for z in [1,2,3,4])
    html = html.replace('{{ZONAS_1F}}', zonas_html)
 
    # Stats full
    se = data.get('stats_equipos', [])
    html = html.replace('{{STATS_FULL}}', render_stats_full(data, fa))
 
    # Goles por tramo
    html = html.replace('{{GOLES_TRAMO}}', render_goles_tramo(data))
 
    # Goleadores
    html = html.replace('{{GOLEADORES}}', render_goleadores(data.get('goleadores', []), fa))
 
    # Proyección completa (2F + Reválida)
    html = html.replace('{{PROY_FULL}}', render_proy_full(data))
    # Compat por si quedara {{ZONAS_2F}} en algún lado
    html = html.replace('{{ZONAS_2F}}',
        render_zona_2f('A', '#0288d1', 'Z1 top5 + Z2 top4', data.get('zona_a_2f', [])) +
        render_zona_2f('B', '#e65100', 'Z3/Z4 top4 + mejor 5to', data.get('zona_b_2f', [])))
 
    # JSON perfiles inyectado en el HTML para uso de JS
    import json as _json
    perfiles_json = _json.dumps(data.get('perfiles', {}), ensure_ascii=False, default=str)
    html = html.replace('{{PERFILES_JSON}}', perfiles_json)
 
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"Dashboard generado: {output_path}")
 
# ═══════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════
if __name__ == '__main__':
    SHEET_ID   = '1s6GRQkIM8bqL3st2eeZT37qSAdT4ElomRQS54KIqa6Q'
    path_carga = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Carga'
    path_goles = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=Goles'
    template   = Path('tfa2026_mini_template.html')
    output     = Path('index.html')
 
    print(f"Leyendo {path_carga}...")
    df_carga = leer_carga(path_carga)
    print(f"Leyendo {path_goles}...")
    df_goles = leer_goles(path_goles)
 
    print("Calculando datos...")
    data  = armar_datos(df_carga, df_goles)
    stats = data['stats']
 
    print(f"  Fase activa  : {stats['fase_activa']}")
    print(f"  Fecha        : {stats['fecha_actual']}/{stats['fecha_total']}")
    print(f"  Partidos     : {stats['partidos_jugados']}")
    print(f"  Goles        : {stats['total_goles']}")
    if data['mejor_5to']:
        m5 = data['mejor_5to']
        print(f"  Mejor 5to    : {m5['nombre']} (Z{m5['from_zone']}, {m5['pts']}pts)")
 
    generar_html(data, template, output)
