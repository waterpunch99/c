"""Synthetic Phase 4 evidence is isolated in temporary directories, never results/."""
import copy
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from phase4_config import load_phase4, cost_plan
from phase4_diagnostics import imbalance, counter_difference, input_validation, phase4_diagnose, paired_partition_comparison, precheck_decision, resource_efficiency
from phase4_engine import execute, comparison_summary
from phase4_analysis import records_for_analysis, repeat_statistics, make_charts


def fixture(path,cfg,run_id='fixture',mode='smoke'):
    path=Path(path); path.mkdir(parents=True,exist_ok=True)
    p=cfg['operator_parallelism']['aggregation']; skew=cfg['distribution']=='skewed'
    processed=[500//p+(i<500%p) for i in range(p)]
    hot=[200 if i==0 and skew else 0 for i in range(p)]
    if skew:
        processed=[200+300//p]+[300//p]* (p-1)
        processed[-1]+=500-sum(processed)
    counts={'generated':500,'sent':500,'acked':500,'hot':200 if skew else 0,'sent_hot':200 if skew else 0,'acked_hot':200 if skew else 0}
    producer={'counts':{'measurement':counts},'acknowledged_partition_counts':{'measurement':{str(i):500//cfg['kafka_partitions']+(i<500%cfg['kafka_partitions']) for i in range(cfg['kafka_partitions'])}}}
    (path/'producer.json').write_text(json.dumps(producer))
    meta={'config':cfg,'mode':mode,'plan':{'measurement_start_wall_ms':0,'measurement_end_wall_ms':20000}}
    (path/'metadata.json').write_text(json.dumps(meta))
    summary={'run_id':run_id,'execution_validity':'VALID','performance_stability':'UNKNOWN','observations':{'measurement_sent_records':500,'restart_count':0,'resource_evidence_missing':False}}
    (path/'summary.json').write_text(json.dumps(summary))
    names={'source':'Kafka Source','parse':'Parse','heavy':'Heavy CPU','aggregation':'Stateful Aggregation','sink':'Measurement Sink'}
    samples=[]
    for t in [1000,7000,13000,19000]:
        subtasks=[]
        for op in names:
            for i in range(cfg['operator_parallelism'][op]):
                metrics={'numRecordsIn':t//10,'numRecordsOut':t//10,'completedRecords':t//10,
                    'busyTimeMsPerSecond':900 if skew and op=='aggregation' and i==0 else 20,
                    'idleTimeMsPerSecond':100 if skew and op=='aggregation' and i==0 else 980,'backPressuredTimeMsPerSecond':0,
                    'operatorCpuTimeSupported':1,'sampledOperatorCpuNanos':t*100,'operatorCpuSampleCount':t//100,
                    'operatorCpuInvalidSamples':0,
                    'aggregationMetricObservedWallMs':t,'aggregationMeasurementRecords':processed[i] if op=='aggregation' else 0,
                    'aggregationMeasurementHotRecords':hot[i] if op=='aggregation' else 0,
                    'aggregationMeasurementWindowRecords':processed[i] if op=='aggregation' else 0,
                    'aggregationMeasurementWindowHotRecords':hot[i] if op=='aggregation' else 0,
                    'hotKeyObservedSubtask':i if op=='aggregation' and hot[i]>0 else -1,
                    'hotKeyObservedKeyGroup':3 if op=='aggregation' and hot[i]>0 else -1}
                subtasks.append({'operator':names[op],'subtask':i,'parallelism':cfg['operator_parallelism'][op],'taskmanager_id':'tm','metrics':metrics})
        resource={'raw':{'wall_time_ms':t,'cpu.stat':{'usage_usec':t*100}},'delta':{'cpu_quota_utilization':.2,'throttled_period_ratio':0,'memory_bytes':1000}}
        samples.append({'started_wall_ms':t,'wall_time_ms':t,'subtasks':subtasks,'cluster_occupied_slots':2,'resources':{'taskmanager':resource,'kafka':resource}})
    (path/'raw_samples.jsonl').write_text(''.join(json.dumps(s)+'\n' for s in samples))
    return summary


class ConfigTest(unittest.TestCase):
    def test_default_cases_and_paired_smoke_cost(self):
        a=load_phase4(3); b=load_phase4(4)
        self.assertEqual(a['workload']['cases']['bottleneck_focused'],{'source':2,'parse':2,'heavy':8,'aggregation':4,'sink':2})
        self.assertEqual(cost_plan(a,3)['maximum_run_count'],4)
        self.assertEqual(cost_plan(b,4)['maximum_run_count'],4)
        self.assertEqual(load_phase4(4,'full')['plan']['parallelisms'],[2,4,8,16])
        self.assertEqual(cost_plan(load_phase4(4,'full'),4)['maximum_run_count'],24)
    def test_invalid_overrides_and_full_repetitions(self):
        for override in [{'parallelisms':[2,2]},{'parallelisms':[0]},{'distributions':['other']},{'hot_key_ratio':float('nan')},{'unknown':1}]:
            with self.assertRaises(ValueError): load_phase4(4,overrides=override)
        with self.assertRaises(ValueError): load_phase4(4,'full',overrides={'repetitions':1})
    def test_single_distribution_is_explicit_and_seed_repeatable(self):
        c=load_phase4(4,overrides={'distributions':['skewed'],'seed':43,'hot_key_ratio':.5})
        self.assertEqual(c['plan']['distributions'],['skewed']); self.assertEqual(c['run_config']['seed'],43)


class DiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.cfg=load_phase4(4); self.run=copy.deepcopy(self.cfg['run_config']); self.run['kafka_partitions']=2
    def diagnostic(self,path,distribution='skewed'):
        c=copy.deepcopy(self.run); c['distribution']=distribution; fixture(path,c)
        return phase4_diagnose(path,self.cfg['evidence_thresholds'])
    def test_zero_mean_and_missing_imbalance_are_not_zero_ratios(self):
        self.assertIsNone(imbalance([0,0])['max_mean_ratio']); self.assertIsNone(imbalance([1,None])['cv'])
        self.assertEqual(imbalance([3,1])['max_mean_ratio'],1.5)
    def test_exact_hot_counter_reconciliation_and_skew_candidate(self):
        with tempfile.TemporaryDirectory() as td:
            d=self.diagnostic(td)
            self.assertEqual(d['aggregation_load']['placement_status'],'OBSERVED_SINGLE_SUBTASK')
            self.assertEqual(d['aggregation_load']['hot_subtasks'],[0])
            self.assertEqual(d['aggregation_load']['processed_hot_ratio'],.4)
            self.assertEqual(d['bottleneck_candidate'],'aggregation')
            self.assertEqual(d['aggregation_load']['processed_cohort']['max_mean_ratio'],1.4)
            self.assertTrue(all(x['sampled_body_cpu_ns_per_record']==10000 for x in d['subtask_details']))
    def test_uniform_no_hot_events_is_not_predicted_placement(self):
        with tempfile.TemporaryDirectory() as td:
            d=self.diagnostic(td,'uniform')
            self.assertEqual(d['aggregation_load']['placement_status'],'NO_HOT_EVENTS')
            self.assertEqual(d['aggregation_load']['hot_subtasks'],[])
            self.assertIsNone(d['bottleneck_candidate'])
    def test_stale_or_restart_evidence_is_inconclusive(self):
        with tempfile.TemporaryDirectory() as td:
            self.diagnostic(td)
            path=Path(td)/'raw_samples.jsonl'; rows=[json.loads(s) for s in path.read_text().splitlines()]
            for r in rows[-1]['subtasks']: r['metrics']['aggregationMetricObservedWallMs']=0
            path.write_text(''.join(json.dumps(s)+'\n' for s in rows))
            d=phase4_diagnose(td,self.cfg['evidence_thresholds'])
            self.assertEqual(d['aggregation_load']['placement_status'],'INCONCLUSIVE')
            self.assertIsNone(d['aggregation_load']['processed_cohort']['max_mean_ratio'])
    def test_multiple_hot_subtasks_detect_contamination(self):
        with tempfile.TemporaryDirectory() as td:
            self.diagnostic(td)
            path=Path(td)/'raw_samples.jsonl'; rows=[json.loads(s) for s in path.read_text().splitlines()]
            agg=[r for r in rows[-1]['subtasks'] if r['operator']=='Stateful Aggregation']
            agg[0]['metrics']['aggregationMeasurementHotRecords']=100
            agg[1]['metrics'].update(aggregationMeasurementHotRecords=100,hotKeyObservedSubtask=1)
            path.write_text(''.join(json.dumps(s)+'\n' for s in rows))
            self.assertEqual(phase4_diagnose(td,self.cfg['evidence_thresholds'])['aggregation_load']['placement_status'],'CONTAMINATED_MULTIPLE_SUBTASKS')
    def test_distribution_validation_missing_and_wrong_ratio(self):
        with tempfile.TemporaryDirectory() as td:
            d=self.diagnostic(td); producer=json.loads((Path(td)/'producer.json').read_text())
            self.assertEqual(d['input_validation']['status'],'VALID')
            producer['counts']['measurement']['hot']=500
            self.assertEqual(input_validation(producer,self.run|{'distribution':'skewed'},self.cfg['evidence_thresholds'])['status'],'INVALID')
            self.assertEqual(input_validation(None,self.run,self.cfg['evidence_thresholds'])['status'],'INCONCLUSIVE')
    def test_counter_reset_and_cpu_missing_never_zero(self):
        rows=[(1,{'metrics':{'x':10}}),(2,{'metrics':{'x':1}})]
        self.assertIsNone(counter_difference(rows,'x')); self.assertIsNone(counter_difference(rows,'missing'))
        with tempfile.TemporaryDirectory() as td:
            self.diagnostic(td)
            p=Path(td)/'raw_samples.jsonl'; samples=[json.loads(s) for s in p.read_text().splitlines()]
            for s in samples:
                for r in s['subtasks']: r['metrics']['operatorCpuTimeSupported']=0
            p.write_text(''.join(json.dumps(s)+'\n' for s in samples))
            self.assertTrue(all(r['sampled_body_cpu_ns_per_record'] is None for r in phase4_diagnose(td,self.cfg['evidence_thresholds'])['subtask_details']))
    def test_partition_pairs_separate_key_distribution_and_resource_budget(self):
        with tempfile.TemporaryDirectory() as td:
            a=self.diagnostic(Path(td)/'u','uniform'); b=self.diagnostic(Path(td)/'s')
            self.assertEqual(paired_partition_comparison(a,b,.02)['status'],'VALID')
            self.assertEqual(paired_partition_comparison(a,b,.02)['total_variation'],0)
            changed=copy.deepcopy(b); changed['config']['operator_parallelism']['source']+=1
            self.assertEqual(paired_partition_comparison(a,changed,.02)['status'],'INVALID')
            changed=copy.deepcopy(b); changed['input_validation']['status']='INVALID'
            self.assertEqual(paired_partition_comparison(a,changed,.02)['status'],'INVALID')
            b['input_validation']['partition_counts']=[500,0]
            self.assertEqual(paired_partition_comparison(a,b,.02)['status'],'INVALID')
            b['comparison_fingerprint']='different'
            self.assertIsNone(paired_partition_comparison(a,b,.02)['total_variation'])
    def test_precheck_unknown_is_only_provisional_and_low_ratio_fails(self):
        cfg=load_phase4(3)
        with tempfile.TemporaryDirectory() as td:
            d=self.diagnostic(td,'uniform'); row={'run_id':'fixture','execution_validity':'VALID','performance_stability':'UNKNOWN','observations':{'measurement_sink_produced_ratio':1,'backlog_regression':{'ci95':[-1,1]}}}
            self.assertEqual(precheck_decision([row],{'fixture':d},cfg)['status'],'PROVISIONAL_SMOKE_ONLY')
            row['observations']['measurement_sink_produced_ratio']=.5
            self.assertEqual(precheck_decision([row],{'fixture':d},cfg)['status'],'FAILED_OR_INCONCLUSIVE')
    def test_resource_core_seconds_missing_and_reset(self):
        a={'wall_time_ms':1000,'resources':{'taskmanager':{'raw':{'wall_time_ms':1000,'cpu.stat':{'usage_usec':10}},'delta':{}}}}
        b={'wall_time_ms':2000,'resources':{'taskmanager':{'raw':{'wall_time_ms':2000,'cpu.stat':{'usage_usec':5}},'delta':{}}}}
        self.assertIsNone(resource_efficiency([a,b],{})['taskmanager_observed_core_seconds'])
    def test_cgroup_timestamp_outside_window_is_excluded_from_cost(self):
        samples=[]
        for observed,actual,usage in [(100,120,1000),(200,220,2000),(290,320,999999)]:
            samples.append({'wall_time_ms':observed,'resources':{'taskmanager':{'raw':{'wall_time_ms':actual,'cpu.stat':{'usage_usec':usage}},'delta':{}}}})
        cost=resource_efficiency(samples,{}, {'measurement_start_wall_ms':100,'measurement_end_wall_ms':300})
        self.assertAlmostEqual(cost['taskmanager_observed_core_seconds'],.001)
        self.assertEqual(cost['cpu_snapshot_count'],2)
        self.assertEqual(cost['observed_interval_seconds'],.1)


class EngineTest(unittest.TestCase):
    def runner(self,cfg,mode,base,output_dir,experiment_id,context):
        rid='fixture_'+str(self.calls); self.calls+=1; path=Path(output_dir)/rid
        result=fixture(path,cfg,rid,mode)
        result.update(result_directory=str(path),cleanup_errors=[])
        result['observations'].update(measurement_sink_produced_ratio=1,backlog_regression={'ci95':[-1,1]},measurement_sink_throughput=100,latency={'p99':255,'p95':255,'p50':127,'sample_count':100})
        self.configs.append(cfg); return result
    def setUp(self): self.calls=0; self.configs=[]
    def test_operator_cases_and_precheck_workload_separation(self):
        with tempfile.TemporaryDirectory() as td,patch('builtins.print'):
            path,s=execute(load_phase4(3),3,Path(td),runner=self.runner)
            self.assertEqual(s['status'],'COMPLETED'); self.assertEqual(s['run_count'],4)
            self.assertEqual(self.configs[0]['cpu_work_iterations'],0)
            self.assertEqual(self.configs[1]['cpu_work_iterations'],200000)
            self.assertEqual(self.configs[3]['operator_parallelism']['heavy'],8)
            self.assertTrue(s['comparison']['case_conditions_comparable'])
            self.assertTrue((path/'precheck_decision.json').exists())
    def test_paired_skew_runs_matching_seeds_and_preserved_unknown(self):
        with tempfile.TemporaryDirectory() as td,patch('builtins.print'):
            path,s=execute(load_phase4(4),4,Path(td),runner=self.runner)
            self.assertEqual(s['run_count'],4); self.assertEqual(len(s['comparison']['pairs']),2)
            self.assertTrue(all(p['status']=='VALID' for p in s['comparison']['pairs']))
            self.assertEqual(self.configs[0]['seed'],self.configs[1]['seed'])
            self.assertTrue(all(c['performance_stability']=='UNKNOWN' for c in s['comparison']['cases']))
            _,second=execute(load_phase4(4,overrides={'distributions':['skewed']}),4,Path(td),runner=self.runner)
            self.assertTrue(all(p['status']=='Not Executed' for p in second['comparison']['pairs']))
    def test_cleanup_failure_stops_next_and_preserves_error(self):
        def fail(*args,**kw): return self.runner(*args,**kw)|{'cleanup_errors':['fixture failure']}
        with tempfile.TemporaryDirectory() as td,patch('builtins.print'):
            p,s=execute(load_phase4(4),4,Path(td),runner=fail)
            self.assertEqual(s['status'],'ERROR'); self.assertEqual(self.calls,1); self.assertTrue((p/'summary.csv').exists())
    @unittest.skipUnless(importlib.util.find_spec('pandas') and importlib.util.find_spec('matplotlib'),'Analysis dependencies absent on host')
    def test_analysis_real_schema_fixture_missing_and_no_overwrite(self):
        from phase4_analysis import analyze
        with tempfile.TemporaryDirectory() as td,patch('builtins.print'):
            p,s=execute(load_phase4(4),4,Path(td),runner=self.runner)
            out=analyze(p,4); second=analyze(p,4)
            self.assertNotEqual(out,second)
            status=json.loads((out/'analysis.json').read_text())
            self.assertEqual(status['charts']['subtask_busy.png']['status'],'Generated')
            self.assertEqual(status['charts']['hot_subtask_load.png']['status'],'Generated')
            self.assertTrue((out/'subtask_diagnostics.csv').exists())
            records=records_for_analysis(read_jsonl(p/'runs.jsonl'),json.loads((p/'diagnostics.json').read_text()),s['comparison'],4)
            self.assertTrue(all(x['std'] is None for x in repeat_statistics(records)))


from result_store import read_jsonl
if __name__=='__main__': unittest.main()
