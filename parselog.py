import os
import re
import csv
import json
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Regex patterns (unchanged)
WSJT_PATTERN = re.compile(
    r'^(\d{6}_\d{6})\s+'      
    r'(\d+\.\d+)\s+'           
    r'(Rx|Tx)\s+'              
    r'(\w+)\s+'                
    r'(-?\d+)\s+'              
    r'(-?\d+\.\d+)\s+'         
    r'(\d+)\s+'                
    r'(.{0,20})'               
)

PYCOM_PATTERN = re.compile(
    r'^(\d{4}-\d{2}-\d{2})\s+'                  
    r'(\d{2}:\d{2})\s+'                         
    r'INFO\s+\[Pycom\.lib\.csnsat\.csnSatManager\]\s+'
    r'Timestamp:\s+(\d{2}:\d{2}:\d{2}),\s+'      
    r'Sat:\s+(\S+),\s+'
    r'Az:\s+([\d\.]+),\s+'
    r'El:\s+([\d\.-]+),\s+'
    r'Range km:\s+([\d\.]+),\s+'
    r'Main:\s+(\d+),\s+'
    r'Sub:\s+(\d+),\s+'
    r'Doppler up:\s+(-?\d+),\s+'
    r'Doppler down:\s+(-?\d+),\s+'
    r'Doppler up rate:\s+(-?\d+),\s+'
    r'Doppler down rate:\s+(-?\d+),\s+'
    r'Offset:\s+(-?\d+),\s+'
    r'Tracking:\s+(\S+),\s+'
    r'Freq Scaling:\s+(\S+),\s+'
    r'RIT:\s+(\S+),\s+'
    r'RIT Freq:\s+(-?\d+)'
)

# File paths (adjust if needed)
WSJT_TX_LOG = Path(r'C:\Users\steve\AppData\Local\WSJT-X\ALL.TXT')
WSJT_RX_LOG = Path(r'C:\Users\steve\AppData\Local\WSJT-X - None\ALL.TXT')
PYCOM_LOG = Path(r'C:\Users\steve\Pycom\logs\pycom.log')

# Output directory
OUTPUT_DIR = Path.cwd() / "echo_passes"
OUTPUT_DIR.mkdir(exist_ok=True)

TIME_TOLERANCE = timedelta(seconds=1)
MAX_TIME_DIFF_FOR_MATCH = 5
MAX_GAP_BETWEEN_ECHOES = timedelta(minutes=30)
PASS_MARGIN = timedelta(minutes=10)  # Extra window around pass for full track


def parse_wsjt_line(line: str):
    match = WSJT_PATTERN.match(line.strip())
    if not match:
        return None
    ts_str, freq_str, rxtx, mode, snr_str, dt_str, f_str, msg = match.groups()
    try:
        yy = 2000 + int(ts_str[:2])
        mm, dd = int(ts_str[2:4]), int(ts_str[4:6])
        hh, mi, ss = int(ts_str[7:9]), int(ts_str[9:11]), int(ts_str[11:13])
        ts = datetime(yy, mm, dd, hh, mi, ss)
        return {
            'ts': ts,
            'ts_str': ts_str,
            'freq': float(freq_str),
            'type': rxtx,
            'mode': mode,
            'snr': int(snr_str),
            'dt': float(dt_str),
            'f': int(f_str),
            'msg': msg.strip()
        }
    except ValueError:
        return None


def load_wsjt_entries(path: Path, entry_type: str):
    entries = []
    if not path.exists():
        print(f"Warning: File not found: {path}")
        return entries
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            parsed = parse_wsjt_line(line)
            if parsed and parsed['type'] == entry_type:
                entries.append(parsed)
    return entries


def find_echoes(tx_entries, rx_entries):
    echoes = []
    for tx in tx_entries:
        for rx in rx_entries:
            if (rx['mode'] != tx['mode'] or
                rx['msg'] != tx['msg'] or
                abs((rx['ts'] - tx['ts']).total_seconds()) > TIME_TOLERANCE.total_seconds() or
                rx['freq'] == tx['freq'] or
                'M0SNZ' not in rx['msg']):
                continue

            echoes.append({
                'ts': tx['ts'],
                'ts_str': tx['ts_str'],
                'tx_freq': tx['freq'],
                'tx_offset': tx['f'],
                'rx_freq': rx['freq'],
                'rx_offset': rx['f'],
                'dt': rx['dt'],
                'snr': rx['snr'],
                'message': tx['msg']
            })
            break
    return sorted(echoes, key=lambda x: x['ts'])


def parse_pycom_log(path: Path):
    entries = []
    if not path.exists():
        print(f"Warning: File not found: {path}")
        return entries
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            m = PYCOM_PATTERN.match(line.strip())
            if not m:
                continue
            log_date = m.group(1)
            inner_time = m.group(3)
            ts = datetime.strptime(f"{log_date} {inner_time}", '%Y-%m-%d %H:%M:%S')
            entries.append({
                'ts': ts,
                'sat': m.group(4),
                'az': float(m.group(5)),
                'el': float(m.group(6)),
                'range_km': float(m.group(7)),
                'main': int(m.group(8)),
                'sub': int(m.group(9)),
                'dop_up': int(m.group(10)),
                'dop_down': int(m.group(11)),
                'dop_up_rate': int(m.group(12)),
                'dop_down_rate': int(m.group(13)),
                'offset': int(m.group(14)),
                'rit': m.group(17),
                'rit_freq': int(m.group(18))
            })
    return sorted(entries, key=lambda x: x['ts'])


def match_echo_to_pycom(echo, pycom_entries):
    if not pycom_entries:
        return None, None
    closest = min(pycom_entries, key=lambda p: abs((p['ts'] - echo['ts']).total_seconds()))
    time_diff = abs((closest['ts'] - echo['ts']).total_seconds())
    return (closest, time_diff) if time_diff < MAX_TIME_DIFF_FOR_MATCH else (None, None)


def group_by_pass(combined_echoes):
    if not combined_echoes:
        return []

    passes = []
    current_pass = [combined_echoes[0]]

    for prev, curr in zip(combined_echoes, combined_echoes[1:]):
        prev_has_pycom = 'pycom' in prev
        curr_has_pycom = 'pycom' in curr

        time_gap_ok = (curr['echo']['ts'] - prev['echo']['ts']) <= MAX_GAP_BETWEEN_ECHOES

        if prev_has_pycom and curr_has_pycom:
            same_sat = prev['pycom']['sat'] == curr['pycom']['sat']
            if same_sat and time_gap_ok:
                current_pass.append(curr)
            else:
                passes.append(current_pass)
                current_pass = [curr]
            continue

        if time_gap_ok:
            current_pass.append(curr)
        else:
            passes.append(current_pass)
            current_pass = [curr]

    passes.append(current_pass)
    return passes


def calculate_pass_stats(pass_group):
    if not pass_group:
        return {}

    valid_items = [item for item in pass_group if 'pycom' in item]
    num_echoes = len(pass_group)
    num_with_sat = len(valid_items)

    if num_with_sat == 0:
        return {
            'num_echoes': num_echoes,
            'note': 'No satellite data available',
            'center_drift': 0
        }

    offset_deltas = [item['echo']['tx_offset'] - item['echo']['rx_offset'] for item in valid_items]
    snrs = [item['echo']['snr'] for item in valid_items]
    dts = [item['echo']['dt'] for item in valid_items]
    azs = [item['pycom']['az'] for item in valid_items]
    els = [item['pycom']['el'] for item in valid_items]
    ranges = [item['pycom']['range_km'] for item in valid_items]
    dop_ups = [item['pycom']['dop_up'] for item in valid_items]
    dop_downs = [item['pycom']['dop_down'] for item in valid_items]
    total_freqs = [item['pycom']['main'] + item['pycom']['sub'] for item in valid_items]
    min_drift = valid_items[0]['pycom']['main'] + valid_items[0]['echo']['rx_offset'] + valid_items[0]['pycom']['sub'] - valid_items[0]['echo']['tx_offset']
    max_drift = valid_items[-1]['pycom']['main'] + valid_items[-1]['echo']['rx_offset'] + valid_items[-1]['pycom']['sub'] - valid_items[-1]['echo']['tx_offset']
    center_drift = round((min_drift + max_drift) / 2)

    return {
        'num_echoes': num_echoes,
        'num_with_sat': num_with_sat,
        'offset_delta_mean_hz': round(np.mean(offset_deltas), 1),
        'offset_delta_std_hz': round(np.std(offset_deltas), 1),
        'snr_mean_db': round(np.mean(snrs), 1),
        'snr_min_db': int(np.min(snrs)),
        'snr_max_db': int(np.max(snrs)),
        'dt_mean_s': round(np.mean(dts), 2),
        'az_mean_deg': round(np.mean(azs), 1),
        'az_min_deg': round(np.min(azs), 1),
        'az_max_deg': round(np.max(azs), 1),
        'el_mean_deg': round(np.mean(els), 1),
        'el_max_deg': round(np.max(els), 1),
        'range_min_km': int(np.min(ranges)),
        'dop_up_mean_hz': int(np.round(np.mean(dop_ups))),
        'dop_down_mean_hz': int(np.round(np.mean(dop_downs))),
        'total_freq_min_hz': int(np.min(total_freqs)),
        'total_freq_max_hz': int(np.max(total_freqs)),
        'total_freq_mean_hz': round(np.mean(total_freqs), 1),
        'total_freq_std_hz': round(np.std(total_freqs), 1),
        'center_drift': center_drift,
    }


def extract_full_pass_track(pass_group, all_pycom_entries):
    """
    Extract all real tracking points from pycom.log for this pass.
    Returns: list of (az, el), start_dt, end_dt, sat_name
    """
    valid_items = [item for item in pass_group if 'pycom' in item]
    if not valid_items:
        return None, None, None, None

    valid_items.sort(key=lambda x: x['echo']['ts'])
    sat_name = valid_items[0]['pycom']['sat']

    start_echo = valid_items[0]['echo']['ts']
    end_echo = valid_items[-1]['echo']['ts']

    window_start = start_echo - PASS_MARGIN
    window_end = end_echo + PASS_MARGIN

    track_points = [
        (p['az'], max(p['el'], 0), p['range_km'], p['main'], p['sub'], p['dop_up'], p['dop_down'])  # clip negative elevation
        for p in all_pycom_entries
        if p['sat'] == sat_name and window_start <= p['ts'] <= window_end
    ]

    if len(track_points) < 2:
        return None, None, None, None

    return track_points, start_echo, end_echo, sat_name


def export_pass(csv_heading, pass_group, pass_num, track_data, center_drift):
    base_name = None
    for item in pass_group:
        if not 'pycom' in item:
            continue
        
        pycom = item['pycom']
        sat_name = pycom['sat']
        start_ts_str = item['echo']['ts_str']
        yy = 2000 + int(start_ts_str[:2])
        mm, dd = int(start_ts_str[2:4]), int(start_ts_str[4:6])
        hh, mi, ss = int(start_ts_str[7:9]), int(start_ts_str[9:11]), int(start_ts_str[11:13])
        start_dt = datetime(yy, mm, dd, hh, mi, ss)

        base_name = start_dt.strftime('%Y%m%d_%H%M%S') + f"_{sat_name}"
        break
        
    if not base_name:
        return

    # CSV export (unchanged columns)
    csv_file = OUTPUT_DIR / f"{base_name}.csv"
    if not os.path.exists(csv_file):
        with csv_file.open('w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(csv_heading)

            for item in pass_group:
                if 'pycom' not in item:
                    continue
                e = item['echo']
                p = item['pycom']
                time_diff = item['time_diff']
                offset_delta = e['tx_offset'] - e['rx_offset']
                drift = p['main'] + e['rx_offset'] + p['sub'] - e['tx_offset'] - center_drift
                rd = drift - p['dop_up'] - p['dop_down']
                writer.writerow([
                    e['ts_str'], e['tx_offset'], e['rx_offset'], offset_delta,
                    e['message'], e['snr'], e['dt'], p['sat'],
                    f"{p['az']:.1f}", f"{p['el']:.1f}", f"{p['range_km']:.0f}",
                    p['main'], p['sub'], p['main'] + p['sub'], p['dop_up'], p['dop_down'], p['offset'],
                    p['rit'], p['rit_freq'], f"{time_diff:.0f}", drift, rd,
                    p['dop_up_rate'], p['dop_down_rate']
                ])

        print(f"  → Exported {len([i for i in pass_group if 'pycom' in i])} echoes to: {csv_file.name}")

    # JSON track export
    if track_data:
        json_file = OUTPUT_DIR / f"{base_name}.json"
        if not os.path.exists(json_file):
            track_dict = {
                "sat": sat_name,
                "start_time": track_data[1].isoformat(),
                "end_time": track_data[2].isoformat(),
                "points": [{"az": az, "el": el, "range_km": range_km, "uplink": sub, "downlink": main, "dop_up": dop_up, "dop_down": dop_down} for az, el, range_km, main, sub, dop_up, dop_down in track_data[0]]
            }
            with json_file.open('w', encoding='utf-8') as f:
                json.dump(track_dict, f, indent=2)
            print(f"  → Saved real tracking path ({len(track_data[0])} points) to: {json_file.name}")


def main():
    print("Loading and processing logs...\n")

    tx_entries = load_wsjt_entries(WSJT_TX_LOG, 'Tx')
    rx_entries = load_wsjt_entries(WSJT_RX_LOG, 'Rx')
    all_pycom_entries = parse_pycom_log(PYCOM_LOG)

    echoes = find_echoes(tx_entries, rx_entries)

    combined = []
    for echo in echoes:
        pycom, time_diff = match_echo_to_pycom(echo, all_pycom_entries)
        if pycom:
            combined.append({
                'echo': echo,
                'pycom': pycom,
                'time_diff': time_diff
            })
        else:
            combined.append({'echo': echo})

    passes = group_by_pass(combined)

    print(f"Found {len(echoes)} total echoes in {len(passes)} pass(es):\n")

    csv_heading = "Timestamp, Tx Offset Hz, Rx Offset Hz, Offset delta Hz, Message, SNR dB, DT s, Sat, Az °, El °, Range km, Main Hz, Sub Hz, Main + Sub Hz, Doppler up Hz, Doppler down Hz, Tuning Offset Hz, RIT, RIT Freq Hz, Time diff s, Drift, Residual Doppler"
    csv_heading = [
                "Timestamp", "Tx Offset Hz", "Rx Offset Hz", "Offset Delta Hz",
                "Message", "SNR dB", "DT s", "Sat", "Az °", "El °", "Range km",
                "Main Hz", "Sub Hz", "Main+Sub Hz", "Doppler up Hz", "Doppler down Hz", "Tuning Offset Hz",
                "RIT", "RIT Freq Hz", "Time diff s", "Drift", "Residual Doppler",
                "Doppler up rate", "Doppler down rate"
            ]

    for i, pass_group in enumerate(passes, 1):
        has_sat_data = any('pycom' in item for item in pass_group)
        first_pycom = next((item['pycom'] for item in pass_group if 'pycom' in item), None)
        sat_name = first_pycom['sat'] if first_pycom else "Unknown"
        start_time = pass_group[0]['echo']['ts_str']

        print(f"\n--- Pass {i}: {sat_name} starting at {start_time} ({len(pass_group)} echoes) ---")

        stats = calculate_pass_stats(pass_group)
        print("\nPass Statistics:")
        if 'note' in stats:
            print(f"   Echoes: {stats['num_echoes']} | {stats['note']}")
        else:
            print(f"   Echoes: {stats['num_echoes']} ({stats['num_with_sat']} with satellite data)")
            print(f"   Offset Δ mean: {stats['offset_delta_mean_hz']} Hz (±{stats['offset_delta_std_hz']})")
            print(f"   SNR: {stats['snr_min_db']} to {stats['snr_max_db']} dB (mean {stats['snr_mean_db']} dB)")
            print(f"   DT mean: {stats['dt_mean_s']} s")
            print(f"   Azimuth: {stats['az_min_deg']}° → {stats['az_max_deg']}° (mean {stats['az_mean_deg']}°)")
            print(f"   Elevation: mean {stats['el_mean_deg']}°, peak {stats['el_max_deg']}°")
            print(f"   Closest range: {stats['range_min_km']} km")
            print(f"   Doppler up mean: {stats['dop_up_mean_hz']} Hz")
            print(f"   Doppler down mean: {stats['dop_down_mean_hz']} Hz")
            print(f"   Total tuned freq: {stats['total_freq_min_hz']} → {stats['total_freq_max_hz']} Hz\n")

        print(",".join(csv_heading))
        for item in pass_group:
            e = item['echo']
            if 'pycom' in item:
                p = item['pycom']
                time_diff = item['time_diff']
                drift = p['main'] + e['rx_offset'] + p['sub'] - e['tx_offset'] - stats['center_drift']
                rd = drift - p['dop_up'] - p['dop_down']
                sat_info = (f", {p['sat']}, {p['az']:.1f}, {p['el']:.1f}, "
                            f"{p['range_km']:.0f}, {p['main']}, {p['sub']}, "
                            f"{p['main'] + p['sub']}, {p['dop_up']}, {p['dop_down']}, {p['offset']}, "
                            f"{p['rit']}, {p['rit_freq']}, {time_diff:.0f}, {drift}, {rd}, "
                            f"{p['dop_up_rate']}, {p['dop_down_rate']}")
            else:
                sat_info = ", No satellite data available"

            offset_delta = e['tx_offset'] - e['rx_offset']
            print(f"{e['ts_str']}, {e['tx_offset']}, {e['rx_offset']}, "
                  f"{offset_delta}, {e['message']}, {e['snr']}, {e['dt']}{sat_info}")

        # Extract real track
        track_data = extract_full_pass_track(pass_group, all_pycom_entries)

        export_pass(csv_heading, pass_group, i, track_data, stats['center_drift'])

    if not echoes:
        print("No echoes found.")

    print(f"\nFiles saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    main()