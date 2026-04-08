"""
gera_relatorio_html.py
======================
Gera relatório HTML detalhado da comparação de estruturas.
Pode ser executado após o script principal ou de forma independente.

Uso:
    python gera_relatorio_html.py [--config config.yaml] [--output relatorio.html]
"""

import argparse
import os
import sys
import html as _html
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import yaml

# Importar funções do script principal
from compara_estrutura_fb2pg import (
    _fb_connect, _pg_connect, _fb_tables, _pg_tables,
    _compare_structure
)


HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Relatório de Migração Firebird → PostgreSQL</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 16px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        header {{
            background: linear-gradient(135deg, #1e3a8a 0%, #3730a3 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            font-weight: 700;
        }}
        
        header p {{
            opacity: 0.9;
            font-size: 1.1em;
        }}
        
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            padding: 40px;
            background: #f8fafc;
        }}
        
        .stat-card {{
            background: white;
            padding: 25px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.07);
            text-align: center;
            transition: transform 0.2s;
        }}
        
        .stat-card:hover {{
            transform: translateY(-5px);
            box-shadow: 0 8px 15px rgba(0,0,0,0.15);
        }}
        
        .stat-value {{
            font-size: 2.5em;
            font-weight: 700;
            margin: 10px 0;
        }}
        
        .stat-label {{
            color: #64748b;
            font-size: 0.9em;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        
        .stat-value.success {{ color: #10b981; }}
        .stat-value.warning {{ color: #f59e0b; }}
        .stat-value.error {{ color: #ef4444; }}
        
        .content {{
            padding: 40px;
        }}
        
        .section {{
            margin-bottom: 40px;
        }}
        
        .section-title {{
            font-size: 1.8em;
            color: #1e293b;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #e2e8f0;
        }}
        
        .filter-tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}
        
        .filter-tab {{
            padding: 10px 20px;
            border: 2px solid #e2e8f0;
            background: white;
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            font-weight: 500;
        }}
        
        .filter-tab:hover {{
            background: #f1f5f9;
        }}
        
        .filter-tab.active {{
            background: #3730a3;
            color: white;
            border-color: #3730a3;
        }}
        
        .table-card {{
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 12px;
            margin-bottom: 20px;
            overflow: hidden;
            transition: all 0.2s;
        }}
        
        .table-card:hover {{
            box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        }}
        
        .table-header {{
            padding: 20px;
            background: #f8fafc;
            border-bottom: 1px solid #e2e8f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .table-name {{
            font-size: 1.3em;
            font-weight: 700;
            color: #1e293b;
        }}
        
        .status-badges {{
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
        }}
        
        .badge {{
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        .badge.ok {{ background: #d1fae5; color: #065f46; }}
        .badge.error {{ background: #fee2e2; color: #991b1b; }}
        .badge.warning {{ background: #fef3c7; color: #92400e; }}
        
        .table-body {{
            padding: 20px;
        }}
        
        .issue-list {{
            list-style: none;
        }}
        
        .issue-item {{
            padding: 12px 16px;
            margin-bottom: 8px;
            background: #fef2f2;
            border-left: 4px solid #ef4444;
            border-radius: 6px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }}
        
        .issue-item.count {{ background: #fee2e2; border-color: #dc2626; }}
        .issue-item.pk {{ background: #fef3c7; border-color: #f59e0b; }}
        .issue-item.fk {{ background: #fef3c7; border-color: #f59e0b; }}
        .issue-item.idx {{ background: #dbeafe; border-color: #3b82f6; }}
        .issue-item.uniq {{ background: #dbeafe; border-color: #3b82f6; }}
        .issue-item.check {{ background: #f3e8ff; border-color: #9333ea; }}
        
        .no-issues {{
            text-align: center;
            padding: 40px;
            color: #10b981;
            font-size: 1.2em;
        }}
        
        footer {{
            background: #1e293b;
            color: white;
            text-align: center;
            padding: 20px;
            font-size: 0.9em;
        }}
        
        @media print {{
            body {{ background: white; padding: 0; }}
            .container {{ box-shadow: none; }}
            .filter-tabs {{ display: none; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔄 Relatório de Migração</h1>
            <p>Firebird → PostgreSQL</p>
            <p style="font-size: 0.9em; margin-top: 10px; opacity: 0.8;">
                Gerado em: {timestamp}
            </p>
        </header>
        
        <div class="stats">
            <div class="stat-card">
                <div class="stat-label">Total de Tabelas</div>
                <div class="stat-value">{total_tables}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">100% OK</div>
                <div class="stat-value success">{perfect_count}</div>
                <div class="stat-label" style="margin-top: 5px;">{perfect_pct}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Com Diferenças</div>
                <div class="stat-value error">{issues_count}</div>
                <div class="stat-label" style="margin-top: 5px;">{issues_pct}%</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Só Firebird</div>
                <div class="stat-value warning">{only_fb_count}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Só PostgreSQL</div>
                <div class="stat-value warning">{only_pg_count}</div>
            </div>
        </div>
        
        <div class="content">
            {issues_section}
            
            {perfect_section}
        </div>
        
        <footer>
            <p>Relatório gerado por compara_estrutura_fb2pg.py</p>
            <p style="margin-top: 5px; opacity: 0.7;">DBA Tools - PostgreSQL & Firebird</p>
        </footer>
    </div>
    
    <script>
        // Filtros interativos
        document.addEventListener('DOMContentLoaded', function() {{
            const tabs = document.querySelectorAll('.filter-tab');
            const cards = document.querySelectorAll('.table-card');
            
            tabs.forEach(tab => {{
                tab.addEventListener('click', function() {{
                    const filter = this.dataset.filter;
                    
                    // Atualizar tabs
                    tabs.forEach(t => t.classList.remove('active'));
                    this.classList.add('active');
                    
                    // Filtrar cards
                    cards.forEach(card => {{
                        if (filter === 'all' || card.dataset.issues.includes(filter)) {{
                            card.style.display = 'block';
                        }} else {{
                            card.style.display = 'none';
                        }}
                    }});
                }});
            }});
        }});
    </script>
</body>
</html>
"""


def generate_html_report(results, only_fb, only_pg, output_path):
    """Gera relatório HTML completo."""
    
    total = len(results)
    perfect = sum(1 for r in results if all([
        r['count_ok'], r['pk_ok'], r['fk_ok'], r['idx_ok'], r['uniq_ok'], r['check_ok']
    ]))
    issues_list = [r for r in results if r['issues']]
    
    # Estatísticas
    stats = {
        'timestamp': datetime.now().strftime('%d/%m/%Y %H:%M:%S'),
        'total_tables': f'{total:,}',
        'perfect_count': f'{perfect:,}',
        'perfect_pct': f'{perfect*100/total:.1f}' if total > 0 else '0.0',
        'issues_count': f'{len(issues_list):,}',
        'issues_pct': f'{len(issues_list)*100/total:.1f}' if total > 0 else '0.0',
        'only_fb_count': f'{len(only_fb):,}',
        'only_pg_count': f'{len(only_pg):,}',
    }
    
    # Seção de problemas
    issues_html = ''
    if issues_list:
        issues_html = '''
        <div class="section">
            <div class="section-title">⚠️ Tabelas com Diferenças</div>
            
            <div class="filter-tabs">
                <div class="filter-tab active" data-filter="all">Todas</div>
                <div class="filter-tab" data-filter="count">COUNT</div>
                <div class="filter-tab" data-filter="pk">PK</div>
                <div class="filter-tab" data-filter="fk">FK</div>
                <div class="filter-tab" data-filter="idx">INDEX</div>
                <div class="filter-tab" data-filter="uniq">UNIQUE</div>
                <div class="filter-tab" data-filter="check">CHECK</div>
            </div>
        '''
        
        for r in issues_list:
            badges = []
            issues_tags = []
            
            if not r['count_ok']:
                badges.append('<span class="badge error">COUNT</span>')
                issues_tags.append('count')
            if not r['pk_ok']:
                badges.append('<span class="badge warning">PK</span>')
                issues_tags.append('pk')
            if not r['fk_ok']:
                badges.append('<span class="badge warning">FK</span>')
                issues_tags.append('fk')
            if not r['idx_ok']:
                badges.append('<span class="badge warning">INDEX</span>')
                issues_tags.append('idx')
            if not r['uniq_ok']:
                badges.append('<span class="badge warning">UNIQUE</span>')
                issues_tags.append('uniq')
            if not r['check_ok']:
                badges.append('<span class="badge warning">CHECK</span>')
                issues_tags.append('check')

            issue_items = []
            for issue in r['issues']:
                css_class = 'issue-item'
                if issue.startswith('COUNT'):
                    css_class += ' count'
                elif issue.startswith('PK'):
                    css_class += ' pk'
                elif issue.startswith('FK'):
                    css_class += ' fk'
                elif issue.startswith('IDX'):
                    css_class += ' idx'
                elif issue.startswith('UNIQUE'):
                    css_class += ' uniq'
                elif issue.startswith('CHECK'):
                    css_class += ' check'

                issue_items.append(f'<li class="{css_class}">{_html.escape(issue)}</li>')

            issues_html += f'''
            <div class="table-card" data-issues="{' '.join(issues_tags)}">
                <div class="table-header">
                    <div class="table-name">{_html.escape(r['table'])}</div>
                    <div class="status-badges">
                        {''.join(badges)}
                    </div>
                </div>
                <div class="table-body">
                    <ul class="issue-list">
                        {''.join(issue_items)}
                    </ul>
                </div>
            </div>
            '''
        
        issues_html += '</div>'
    
    # Seção de tabelas OK
    perfect_list = [r for r in results if not r['issues']]
    perfect_html = ''
    if perfect_list:
        perfect_html = f'''
        <div class="section">
            <div class="section-title">✅ Tabelas 100% OK ({len(perfect_list)})</div>
            <div class="no-issues">
                {', '.join([r['table'] for r in perfect_list[:50]])}
                {'<br><br>... e mais ' + str(len(perfect_list) - 50) + ' tabelas' if len(perfect_list) > 50 else ''}
            </div>
        </div>
        '''
    
    # Gerar HTML final
    html = HTML_TEMPLATE.format(
        **stats,
        issues_section=issues_html,
        perfect_section=perfect_html
    )
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f'\n✅ Relatório HTML gerado: {output_path}')


def main():
    parser = argparse.ArgumentParser(
        description='Gera relatório HTML da comparação Firebird vs PostgreSQL'
    )
    parser.add_argument('--config', default='config.yaml', help='Caminho do config.yaml')
    parser.add_argument('--schema', default=None, help='Schema PostgreSQL')
    parser.add_argument('--output', default='relatorio_migracao.html', help='Arquivo HTML de saída')
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        sys.exit(f'Erro: config não encontrado: {config_path}')

    with open(config_path, encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    schema = args.schema or cfg.get('postgresql', {}).get('schema', 'public')

    print('Conectando aos bancos de dados...')
    fb_conn = _fb_connect(cfg)
    pg_conn = _pg_connect(cfg)

    print('Coletando informações...')
    fb_tables_raw = _fb_tables(fb_conn)
    pg_tables_raw = _pg_tables(pg_conn, schema)

    fb_map = {t.lower(): t for t in fb_tables_raw}
    pg_map = {t.lower(): t for t in pg_tables_raw}

    common_keys = sorted(set(fb_map) & set(pg_map))
    only_fb = sorted(set(fb_map) - set(pg_map))
    only_pg = sorted(set(pg_map) - set(fb_map))

    results = []
    total = len(common_keys)
    
    print(f'Comparando {total} tabelas...')
    for i, key in enumerate(common_keys, 1):
        if i % 50 == 0:
            print(f'  {i}/{total}...')
        
        fb_name = fb_map[key]
        pg_name = pg_map[key]
        result = _compare_structure(fb_conn, pg_conn, schema, key, fb_name, pg_name)
        results.append(result)

    fb_conn.close()
    pg_conn.close()

    print('Gerando relatório HTML...')
    generate_html_report(results, only_fb, only_pg, args.output)


if __name__ == '__main__':
    main()