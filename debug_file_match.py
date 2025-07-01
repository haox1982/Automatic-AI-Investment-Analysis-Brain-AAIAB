#!/usr/bin/env python3
import pathlib

# 检查HTML文件
plot_dir = pathlib.Path('/Volumes/ext-fx/Coding/6.Docker/8.n8n/data_folder/Backtrader/Core/plot_html')
html_files = list(plot_dir.glob('*.html'))

print('所有HTML文件:')
for f in html_files:
    if 'technical_analysis' in f.name:
        print(f'  {f.name}')

print('\n检查央行利率文件匹配:')
filename = '各国央行利率_technical_analysis.html'
print(f'文件名: {filename}')
print(f'小写: {filename.lower()}')

# 检查关键词匹配
keywords = ['利率', '债券', '国债', '收益率', '短期利率', '长期利率', '央行利率', '各国央行利率']
print('\n关键词匹配结果:')
for keyword in keywords:
    match = keyword.lower() in filename.lower()
    print(f'  "{keyword}" -> {match}')

# 检查实际文件是否存在
central_bank_file = plot_dir / filename
print(f'\n文件是否存在: {central_bank_file.exists()}')