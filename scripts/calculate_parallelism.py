"""Arithmetic only: caller-supplied capacity is an assumption, not a measurement."""
import argparse
import json
import math
from decimal import Decimal, ROUND_CEILING


def calculate(peak_input_rate, subtask_capacity, target_utilization, operator='unspecified'):
    for name, value in [('peak_input_rate', peak_input_rate), ('subtask_capacity', subtask_capacity),
                        ('target_utilization', target_utilization)]:
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError('Expected finite positive ' + name)
    if target_utilization > 1:
        raise ValueError('target_utilization must be <=1')
    # Decimal prevents an exact integer boundary from floating-point rounding up.
    p = int((Decimal(str(peak_input_rate)) / (Decimal(str(subtask_capacity)) * Decimal(str(target_utilization)))).to_integral_value(rounding=ROUND_CEILING))
    total = p * subtask_capacity
    return {'operator': operator, 'peak_input_rate': peak_input_rate, 'subtask_capacity': subtask_capacity,
            'target_utilization': target_utilization, 'required_parallelism': p,
            'estimated_load_per_subtask': peak_input_rate / p,
            'estimated_capacity_utilization': peak_input_rate / total,
            'estimated_headroom_records_per_second': total - peak_input_rate,
            'estimated_headroom_fraction': 1 - peak_input_rate / total,
            'assumptions': ['Capacity is supplied by caller; this function does not measure it',
                            'uniform load; capacity transfers to proposed P under same workload/resources',
                            'lambda is this operator input rate; utilization is not Busy Time or CPU',
                            'partitions, keys, slots and physical resource contention require independent validation']}


def calculate_operators(operators):
    return {name: calculate(v['peak_input_rate'], v['subtask_capacity'], v['target_utilization'], name)
            for name, v in operators.items()}


def neighbors(predicted):
    if type(predicted) is not int or predicted < 1:
        raise ValueError('positive integer prediction required')
    return sorted({p for p in [predicted - 2, predicted - 1, predicted, predicted + 1, predicted + 3] if p > 0})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--peak-input-rate', type=float, required=True)
    parser.add_argument('--subtask-capacity', type=float, required=True)
    parser.add_argument('--target-utilization', type=float, required=True)
    parser.add_argument('--operator', default='unspecified')
    args = parser.parse_args()
    print(json.dumps(calculate(args.peak_input_rate, args.subtask_capacity, args.target_utilization, args.operator), indent=2))
