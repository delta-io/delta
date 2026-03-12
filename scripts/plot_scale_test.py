import matplotlib.pyplot as plt
import numpy as np

files =       [652, 6612, 32461, 61843, 315388, 653933, 3197657, 6386680]
legacy_ms =   [28772, 21003, 22230, 23034, 34474, 51028, 177283, 175424]
dist_ms =     [91859, 44402, 40250, 42840, 62560, 93256, 297295, 561439]
legacy_peak = [755, 737, 810, 724, 1045, 1381, 2046, 2046]
dist_peak =   [1988, 1525, 1372, 1419, 1419, 1890, 2024, 2029]
legacy_oom =  [False, False, False, False, False, False, True, True]

files_k = [f / 1000 for f in files]
legacy_s = [t / 1000 for t in legacy_ms]
dist_s = [t / 1000 for t in dist_ms]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

# --- Plot 1: Peak Memory ---
ax1.plot(files_k, legacy_peak, 'o-', color='#e74c3c', linewidth=2.5, markersize=8, label='Legacy', zorder=5)
ax1.plot(files_k, dist_peak, 's-', color='#2ecc71', linewidth=2.5, markersize=8, label='Distributed', zorder=5)
ax1.axhline(y=2048, color='#7f8c8d', linestyle='--', linewidth=1.5, label='2 GB heap limit')

for i, oom in enumerate(legacy_oom):
    if oom:
        ax1.annotate('OOM', (files_k[i], legacy_peak[i]),
                     textcoords="offset points", xytext=(0, 15),
                     ha='center', fontsize=11, fontweight='bold', color='#e74c3c')

ax1.set_xlabel('Number of Files (thousands)', fontsize=13)
ax1.set_ylabel('Peak Driver Memory (MB)', fontsize=13)
ax1.set_title('Driver Memory Usage vs Table Size', fontsize=15, fontweight='bold')
ax1.set_xscale('log')
ax1.legend(fontsize=12, loc='upper left')
ax1.grid(True, alpha=0.3)
ax1.set_ylim(400, 2300)
ax1.tick_params(labelsize=11)

# --- Plot 2: Elapsed Time ---
ax2.plot(files_k, legacy_s, 'o-', color='#e74c3c', linewidth=2.5, markersize=8, label='Legacy', zorder=5)
ax2.plot(files_k, dist_s, 's-', color='#2ecc71', linewidth=2.5, markersize=8, label='Distributed', zorder=5)

for i, oom in enumerate(legacy_oom):
    if oom:
        ax2.annotate('OOM', (files_k[i], legacy_s[i]),
                     textcoords="offset points", xytext=(0, 15),
                     ha='center', fontsize=11, fontweight='bold', color='#e74c3c')

ax2.set_xlabel('Number of Files (thousands)', fontsize=13)
ax2.set_ylabel('Elapsed Time (seconds)', fontsize=13)
ax2.set_title('Streaming Latency vs Table Size', fontsize=15, fontweight='bold')
ax2.set_xscale('log')
ax2.legend(fontsize=12, loc='upper left')
ax2.grid(True, alpha=0.3)
ax2.tick_params(labelsize=11)

fig.suptitle('Delta Streaming Initial Snapshot: Legacy vs Distributed Path\n(Driver heap = 2 GB, AvailableNow trigger)',
             fontsize=16, fontweight='bold', y=1.02)
plt.tight_layout()
plt.savefig('/home/xin.huang/delta/scripts/scale_test_results.png', dpi=150, bbox_inches='tight')
print("Saved to /home/xin.huang/delta/scripts/scale_test_results.png")
