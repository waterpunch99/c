"""Pure measurement/status functions. No Docker and no fabricated benchmark values."""
import math
import statistics


def regression(points):
    usable = [(float(t), float(v)) for t, v in points if v is not None and math.isfinite(float(v))]
    if len(usable) < 3 or len({t for t, _ in usable}) < 3:
        return {'slope': None, 'standard_error': None, 'ci95': None, 'samples': len(usable)}
    origin = usable[0][0]
    xs = [t - origin for t, _ in usable]; ys = [v for _, v in usable]
    mx = statistics.mean(xs); my = statistics.mean(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    residuals = sum((y - my - slope * (x - mx)) ** 2 for x, y in zip(xs, ys))
    error = math.sqrt(residuals / (len(xs) - 2) / sxx)
    # Normal approximation, explicitly not a small-sample Student-t interval.
    return {'slope': slope, 'standard_error': error, 'ci95': [slope - 1.96 * error, slope + 1.96 * error],
            'ci_method': 'normal approximation; temporal independence not established', 'samples': len(xs),
            'start_seconds': usable[0][0], 'end_seconds': usable[-1][0], 'initial': ys[0], 'final': ys[-1],
            'delta': ys[-1] - ys[0]}


def histogram_quantiles(buckets, minimum=None, maximum=None):
    if buckets is None or any(v is None or v < 0 for v in buckets):
        return {'sample_count': None, 'p50': None, 'p95': None, 'p99': None, 'min': minimum, 'max': maximum}
    count = sum(buckets)
    result = {'sample_count': count, 'min': minimum if count else None, 'max': maximum if count else None,
              'method': 'merged log2 histogram upper bounds'}
    for name, q in [('p50', .5), ('p95', .95), ('p99', .99)]:
        result[name] = None
        cumulative = 0
        for i, n in enumerate(buckets):
            cumulative += n
            if count and cumulative >= math.ceil(count * q):
                result[name] = (1 << i) - 1
                break
    return result


def counter_rate(previous, current, seconds):
    if previous is None or current is None or seconds <= 0 or current < previous:
        return None
    return (current - previous) / seconds


def partition_imbalance(counts, partitions):
    if counts is None or partitions <= 0:
        return {'max_mean_ratio': None, 'cv': None}
    values = [counts.get(str(i), counts.get(i, 0)) for i in range(partitions)]
    # This zero means a known partition had zero deliveries in an observed distribution, not a missing metric.
    mean = statistics.mean(values)
    return {'max_mean_ratio': max(values) / mean if mean else None,
            'cv': statistics.pstdev(values) / mean if mean else None, 'counts': values}


def producer_decision(producer, telemetry, cfg):
    reasons = []
    c = producer['counts'].get('measurement', {})
    rate = c.get('sent', 0) / producer['plan']['measurement_seconds']
    expected = cfg['target_rate']
    tolerance = cfg['thresholds']['producer_rate_tolerance']
    if abs(rate / expected - 1) > tolerance:
        reasons.append('actual sent rate outside target tolerance')
    if c.get('acked', 0) != c.get('sent', 0) or producer.get('pending_after_flush'):
        reasons.append('Kafka ACKs do not match sent cohort')
    if producer.get('acknowledged_window_rate') is not None and abs(producer['acknowledged_window_rate'] / expected - 1) > tolerance:
        reasons.append('Kafka ACK rate within measurement window outside target tolerance')
    for name in ['errors', 'queue_full', 'expired']:
        if c.get(name, 0):
            reasons.append('producer ' + name)
    if producer.get('interrupted'):
        reasons.append('producer interrupted')
    plan = producer['plan']
    intervals = [r['rates']['measurement']['sent'] for r in telemetry if r.get('kind') == 'sample'
                 and r['wall_time_ms'] - r['interval_seconds'] * 1000 >= plan['measurement_start_wall_ms']
                 and r['wall_time_ms'] <= plan['measurement_end_wall_ms']]
    if intervals and min(intervals) < expected * cfg['thresholds']['minimum_interval_rate_ratio']:
        reasons.append('producer has intervals below configured minimum rate')
    imbalance = partition_imbalance(producer['acknowledged_partition_counts'].get('measurement'), cfg['kafka_partitions'])
    if imbalance['max_mean_ratio'] is not None and imbalance['max_mean_ratio'] > cfg['thresholds']['partition_max_mean_ratio']:
        reasons.append('Kafka partition imbalance exceeds allowed ratio')
    return {'valid': not reasons, 'reasons': reasons, 'actual_sent_rate': rate,
            'acknowledged_cohort_rate': c.get('acked', 0) / plan['measurement_seconds'],
            'minimum_observed_interval_sent_rate': min(intervals) if intervals else None,
            'rate_interval_count': len(intervals), 'partition_distribution': imbalance}


def classify(observed, thresholds, mode):
    reasons = []; limitations = []; warnings = observed.get('resource_warnings', [])
    result = {'execution_validity': 'VALID', 'performance_stability': 'UNKNOWN', 'reasons': reasons,
              'thresholds': thresholds.copy(), 'observations': observed, 'limitations': limitations,
              'sustainable_throughput': None, 'resource_warnings': warnings}
    if observed.get('execution_errors') or observed.get('job_failed') or observed.get('metric_collection_failed'):
        result['execution_validity'] = 'ERROR'; reasons.append('execution or required metric collection failure'); return result
    if not observed.get('producer_valid', False) or not observed.get('preflight_valid', False):
        result['execution_validity'] = 'INVALID'; reasons.append('producer/preflight assumptions not met'); return result
    if observed.get('config_mismatch') or observed.get('data_contamination') or observed.get('restart_count', 0):
        result['execution_validity'] = 'INVALID'; reasons.append('configuration/data/restart contamination'); return result
    if observed.get('required_missing') or observed.get('restart_count') is None:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('required evidence missing'); return result
    lag = observed.get('backlog_regression', {})
    if lag.get('slope') is None:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('insufficient backlog regression samples'); return result
    increasing = lag['ci95'][0] > thresholds['backlog_slope_records_per_second'] and lag['delta'] > thresholds['backlog_delta_records']
    latency = observed.get('latency_regression', {})
    latency_increasing = latency.get('ci95') is not None and observed.get('clock_trusted', False) and latency['ci95'][0] > thresholds['latency_p95_slope_ms_per_second']
    failures = observed.get('checkpoint_failures')
    if increasing or latency_increasing or (failures is not None and failures > 0):
        result['performance_stability'] = 'UNSTABLE'
        reasons.append('measured backlog growth' if increasing else 'latency growth or checkpoint failures')
        return result
    if mode == 'smoke' or observed['measurement_seconds'] < thresholds['minimum_stability_seconds']:
        reasons.append('short Smoke/measurement is not sustainable capacity evidence'); return result
    if lag['samples'] < thresholds['minimum_samples'] or observed.get('sample_coverage', 0) < thresholds['minimum_sample_coverage']:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('insufficient sample coverage'); return result
    if lag['ci95'][1] > thresholds['backlog_slope_records_per_second'] or lag['delta'] > thresholds['backlog_delta_records']:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('backlog noise does not establish stability'); return result
    if observed.get('checkpoint_completed') in [None, 0] or observed.get('checkpoint_failures') is None:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('checkpoint evidence insufficient'); return result
    if not observed.get('clock_trusted') or observed.get('latency_samples') is None or observed['latency_samples'] < thresholds['minimum_latency_samples'] or latency.get('slope') is None:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('latency clock/sample evidence insufficient'); return result
    if latency.get('ci95') is None or latency['ci95'][1] > thresholds['latency_p95_slope_ms_per_second']:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('latency noise does not establish stability'); return result
    ratio = observed.get('measurement_sink_produced_ratio')
    if ratio is None or ratio < thresholds['sink_produced_ratio']:
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('sink window rate insufficient; drain excluded'); return result
    if observed.get('resource_evidence_missing'):
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('resource saturation evidence missing'); return result
    if observed.get('measurement_cohort_unprocessed', 0) > 0 or not observed.get('latency_cohort_complete', True):
        result['execution_validity'] = 'INCONCLUSIVE'; reasons.append('undrained cohort censors latency and completeness'); return result
    result['performance_stability'] = 'STABLE'
    result['sustainable_throughput'] = observed['measurement_sink_throughput']
    reasons.append('producer, backlog, latency, checkpoint and resources satisfy registered criteria')
    limitations.extend(['observed load is a stable lower bound, not maximum capacity', 'regression CI is approximate and samples can be autocorrelated'])
    return result
