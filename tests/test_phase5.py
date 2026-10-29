"""Phase 5 fixtures live only in TemporaryDirectory, never measured results/."""
import copy
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from phase5_config import load_phase5, cost_plan
from phase5_diagnostics import assignment_snapshot, phase5_diagnose
from phase5_engine import execute
from phase5_analysis import records_for_analysis, repeat_statistics, make_charts, analyze
from benchmark_suite import suite_plan, execute_suite
from blog_report import select_sessions, build_report
import run_once
import preflight as preflight_module
import probe_transport
from test_phase4 import fixture


def assignment_fixture(p=4,partitions=2):
    rows=[]
    for i in range(p):
        # Explicit synthetic callback observations; this is not a real placement prediction.
        ids=[i] if i<partitions else []
        rows.append({'operator':'Kafka Source','subtask':i,'taskmanager_id':'fixture-tm',
            'metrics':{'sourceAssignedPartitionCount':len(ids),'sourceAssignmentSnapshotTimestampMs':1000,
                'numRecordsOut':10 if ids else 0,**{'sourceAssignedPartition'+str(k):1 for k in ids}}})
    return {'wall_time_ms':1100,'subtasks':rows}


def run_fixture(path,cfg,rid='fixture'):
    summary=fixture(path,cfg,rid)
    file=Path(path)/'raw_samples.jsonl'; samples=[json.loads(line) for line in file.read_text().splitlines()]
    for sample in samples:
        for st in sample['subtasks']:
            if st['operator']!='Kafka Source': continue
            i=st['subtask']; ids=[k for k in range(cfg['kafka_partitions']) if k%cfg['operator_parallelism']['source']==i]
            st['metrics'].update({'sourceAssignedPartitionCount':len(ids),'sourceAssignmentSnapshotTimestampMs':sample['wall_time_ms'],
                **{'sourceAssignedPartition'+str(k):1 for k in ids}})
    file.write_text(''.join(json.dumps(s)+'\n' for s in samples))
    return summary


class ConfigTest(unittest.TestCase):
    def test_default_fixed_downstream_and_full_repeats(self):
        cfg=load_phase5(); self.assertEqual(cfg['plan']['parallelisms'],[2,4,8,16])
        self.assertEqual(cfg['run_config']['kafka_partitions'],8)
        self.assertEqual(cfg['run_config']['operator_parallelism'],{'source':2,'parse':8,'heavy':8,'aggregation':8,'sink':8})
        self.assertEqual(cost_plan(cfg)['maximum_run_count'],4)
        self.assertEqual(cost_plan(load_phase5('full'))['maximum_run_count'],12)
    def test_bad_config_not_silent(self):
        for o in [{'parallelisms':[]},{'parallelisms':[2,2]},{'parallelisms':[0]},{'target_rate':float('nan')},{'unexpected':1}]:
            with self.assertRaises(ValueError): load_phase5(overrides=o)
        with self.assertRaises(ValueError): load_phase5('full',overrides={'repetitions':1})
    def test_suite_cost_and_small_explicit_matrix(self):
        self.assertEqual(suite_plan('smoke')['maximum_run_count'],20)
        self.assertEqual(suite_plan('smoke',ROOT/'configs/experiments/suite-quick-smoke.json')['maximum_run_count'],14)


class ProbeTransportTest(unittest.TestCase):
    def response(self,stdout,stderr='',code=0): return subprocess.CompletedProcess(['fixture'],code,stdout,stderr)
    def test_stderr_diagnostics_do_not_corrupt_machine_json(self):
        diagnostics=[]
        with patch.object(probe_transport.subprocess,'run',return_value=self.response('{"fixture":1}','fixture warning')):
            self.assertEqual(probe_transport.query_json(['fixture'],diagnostics=diagnostics.append),{'fixture':1})
        self.assertEqual(diagnostics[0]['stderr'],'fixture warning'); self.assertIsNone(diagnostics[0]['error'])
    def test_empty_reply_retried_once_and_raw_failure_preserved(self):
        diagnostics=[]
        with patch.object(probe_transport.subprocess,'run',side_effect=[self.response(''),self.response('{"fixture":1}')]) as call:
            self.assertEqual(probe_transport.query_json(['fixture'],diagnostics=diagnostics.append),{'fixture':1}); self.assertEqual(call.call_count,2)
        self.assertEqual(diagnostics[0]['stdout'],''); self.assertIn('malformed',diagnostics[0]['error'])
    def test_exhausted_or_nonzero_reply_is_error_not_missing_zero(self):
        for replies,expected in [([self.response('invalid'),self.response('invalid')],2),([self.response('',code=1)],1)]:
            with patch.object(probe_transport.subprocess,'run',side_effect=replies) as call:
                with self.assertRaises(RuntimeError): probe_transport.query_json(['fixture'])
                self.assertEqual(call.call_count,expected)


class AssignmentTest(unittest.TestCase):
    def check(self,s): return assignment_snapshot(s,2,4,3000,0)
    def test_observed_zero_proves_idle_assignment_not_zero_records_alone(self):
        result=self.check(assignment_fixture())
        self.assertEqual(result['status'],'OBSERVED_COMPLETE'); self.assertEqual(result['unassigned_subtasks'],[2,3])
        self.assertEqual(result['partition_owners'],{'0':0,'1':1})
        s=assignment_fixture(); del s['subtasks'][2]['metrics']['sourceAssignedPartitionCount']
        result=self.check(s); self.assertEqual(result['status'],'INCONCLUSIVE'); self.assertIsNone(result['unassigned_subtasks'])
    def test_stale_missing_inventory_and_restart_are_inconclusive(self):
        s=assignment_fixture(); s['wall_time_ms']=10000
        self.assertEqual(self.check(s)['status'],'INCONCLUSIVE')
        s=assignment_fixture(); s['subtasks'].pop()
        self.assertEqual(self.check(s)['status'],'INCONCLUSIVE')
        self.assertEqual(assignment_snapshot(assignment_fixture(),2,4,3000,1)['status'],'INCONCLUSIVE')
    def test_duplicate_owner_and_read_conflict_are_invalid(self):
        s=assignment_fixture(); s['subtasks'][1]['metrics']['sourceAssignedPartition0']=1
        s['subtasks'][1]['metrics']['sourceAssignedPartitionCount']=2
        self.assertEqual(self.check(s)['status'],'INVALID')
        s=assignment_fixture(); s['subtasks'][3]['metrics']['sourceReadNextOffsetPartition1']=1
        self.assertEqual(self.check(s)['status'],'INVALID')
    def test_missing_partition_does_not_make_expected_ownership(self):
        s=assignment_fixture(); del s['subtasks'][1]['metrics']['sourceAssignedPartition1']
        s['subtasks'][1]['metrics']['sourceAssignedPartitionCount']=0
        self.assertEqual(self.check(s)['status'],'INCONCLUSIVE'); self.assertEqual(self.check(s)['partition_owners'],{})
    def test_raw_diagnosis_and_conditional_downstream_check(self):
        with tempfile.TemporaryDirectory() as td:
            cfg=load_phase5(); run=copy.deepcopy(cfg['run_config']); run['kafka_partitions']=2; run['operator_parallelism']['source']=4
            run_fixture(td,run); d=phase5_diagnose(td,cfg)
            self.assertEqual(d['source_assignment']['status'],'OBSERVED_COMPLETE')
            self.assertEqual(d['source_assignment']['unassigned_subtasks'],[2,3])
            self.assertEqual(d['source_comparison_premises']['status'],'VALID')
            self.assertFalse(d['source_comparison_premises']['source_saturation_observed'])


class EngineTest(unittest.TestCase):
    def test_preflight_errors_stop_before_job_and_propagate_cleanup_and_interrupt(self):
        for precheck in [{'valid':False,'reasons':['fixture cleanup'],'cleanup_errors':['fixture topic remains']},
                         {'valid':False,'reasons':['fixture interrupt'],'interrupted':True},
                         {'valid':False,'reasons':['fixture transport'],'error':'fixture API failure'}]:
            with tempfile.TemporaryDirectory() as td, patch.object(run_once,'command',return_value=''), \
                 patch.object(run_once,'rest',side_effect=lambda base,path: {'jobs':[]} if path=='/jobs/overview' else {'slots-available':16}), \
                 patch.object(run_once,'environment',return_value={'clock_probes':{}}), \
                 patch.object(run_once,'preflight',return_value=precheck), \
                 patch.object(run_once,'cleanup_run',side_effect=lambda *args: ({},[])), \
                 patch.object(run_once,'submit') as submit, patch('builtins.print'):
                result=run_once.run(load_phase5()['run_config'],'smoke','http://fixture',Path(td))
                self.assertEqual(result['execution_validity'],'ERROR'); submit.assert_not_called()
                if precheck.get('cleanup_errors'): self.assertEqual(result['cleanup_errors'],['preflight: fixture topic remains'])
                if precheck.get('interrupted'): self.assertTrue(any('KeyboardInterrupt' in e for e in result['observations']['execution_errors']))
    def test_preflight_exception_after_rate_success_revokes_validity(self):
        with tempfile.TemporaryDirectory() as td:
            def command(args,timeout):
                directory=next(Path(td).iterdir())
                (directory/'producer.json').write_text(json.dumps({'counts':{'measurement':{'acked':1}}}))
                return ''
            with patch.object(preflight_module,'create_topic'), patch.object(preflight_module,'command',side_effect=command), \
                 patch.object(preflight_module,'session_args',return_value=[]), \
                 patch.object(preflight_module,'producer_decision',return_value={'valid':True,'reasons':[]}), \
                 patch.object(preflight_module,'read_jsonl',return_value=[]), \
                 patch.object(preflight_module,'cleanup_run',return_value=({},[])):
                result=preflight_module.preflight(load_phase5()['run_config'],Path(td))
                self.assertFalse(result['valid']); self.assertIn('error',result)
    def test_source_only_scaling_and_cleanup_stop(self):
        with tempfile.TemporaryDirectory() as td:
            calls=[]
            def runner(cfg,mode,base,path,experiment_id,context):
                calls.append(copy.deepcopy(cfg)); rid='fixture-'+str(len(calls)); directory=Path(path)/rid
                result=run_fixture(directory,cfg,rid)
                return result | {'result_directory':str(directory),'cleanup_errors':['fixture cleanup failure'] if len(calls)==2 else []}
            directory,summary=execute(load_phase5(),5,td,runner=runner)
            self.assertEqual(summary['status'],'ERROR'); self.assertEqual(summary['run_count'],2)
            self.assertEqual([c['operator_parallelism']['source'] for c in calls],[2,4])
            self.assertTrue(all(c['operator_parallelism']['sink']==8 for c in calls))
            self.assertEqual(len((directory/'runs.jsonl').read_text().splitlines()),2)
    def test_suite_dispatch_and_stop_on_error_preserves_sessions(self):
        def executors(td,fail=None):
            def executor(cfg,e,out,base):
                directory=Path(td)/str(e); directory.mkdir()
                (directory/'runs.jsonl').write_text('')
                return directory,{'session_id':'fixture-'+str(e),'status':'ERROR' if e==fail else 'COMPLETED',
                    'run_count':1,'errors':['fixture failure'] if e==fail else []}
            return {e:executor for e in range(1,6)}
        with tempfile.TemporaryDirectory() as td:
            _,s=execute_suite(suite_plan('smoke'),td,executors=executors(td))
            self.assertEqual(s['status'],'COMPLETED'); self.assertEqual(len(s['sessions']),5)
        with tempfile.TemporaryDirectory() as td:
            directory,s=execute_suite(suite_plan('smoke'),td,executors=executors(td,3))
            self.assertEqual(s['status'],'ERROR'); self.assertEqual(s['not_executed_experiments'],[4,5])
            self.assertTrue((directory/'suite_summary.json').exists())
    def test_cli_five_and_all_plans_do_not_start_load(self):
        for experiment in ['5','all']:
            result=subprocess.run([sys.executable,str(ROOT/'scripts/run_experiment.py'),'--experiment',experiment,'--dry-run'],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr); self.assertIn('maximum_run_count',result.stdout)


class ReportTest(unittest.TestCase):
    def test_one_repeat_has_missing_std_and_invalid_excluded(self):
        base={'parallelism':2,'target_rate':100,'mode':'smoke','condition_fingerprint':'fixture','execution_validity':'VALID','performance_stability':'UNKNOWN','comparison_premises':'VALID','source_throughput':100}
        rows=repeat_statistics([base,base|{'execution_validity':'INVALID','source_throughput':999}])
        actual=next(r for r in rows if r['metric']=='source_throughput')
        self.assertEqual(actual['mean'],100); self.assertEqual(actual['valid_runs'],1); self.assertIsNone(actual['std'])
    def test_not_executed_report_without_fabricated_charts(self):
        with tempfile.TemporaryDirectory() as td:
            out=build_report(td,'smoke',{},{}); report=(out/'blog-report.md').read_text()
            self.assertEqual(report.count('## '),8); self.assertIn('Not Executed',report)
            self.assertFalse(list(out.glob('*.png'))); self.assertEqual(json.loads((out/'report.json').read_text())['run_count'],0)
    def test_selection_filters_modes_and_unfinished_sessions(self):
        with tempfile.TemporaryDirectory() as td:
            for name,mode,start,finished in [('old','smoke',1,True),('new','full',2,True),('unfinished','smoke',3,False)]:
                path=Path(td)/'experiment_5'/('session_'+name); path.mkdir(parents=True)
                (path/'session.json').write_text(json.dumps({'mode':mode,'started_wall_ms':start}))
                if finished: (path/'session_summary.json').write_text('{}')
            self.assertEqual(select_sessions(td,'smoke')[5].name,'session_old')
    def test_suite_rejects_mode_mix(self):
        with tempfile.TemporaryDirectory() as td:
            (Path(td)/'suite_summary.json').write_text(json.dumps({'mode':'full','sessions':[]}))
            with self.assertRaises(ValueError): select_sessions(td,'smoke',td)
    def test_suite_paths_remain_portable_between_host_and_analyzer(self):
        with tempfile.TemporaryDirectory() as td:
            suite=Path(td)/'suites'/'suite_fixture'; suite.mkdir(parents=True)
            session=Path(td)/'experiment_5'/'session_fixture'; session.mkdir(parents=True)
            record={'experiment':5,'session_id':'session_fixture','mode':'smoke','session_directory':'/missing-host/results/experiment_5/session_fixture'}
            (suite/'suite_summary.json').write_text(json.dumps({'mode':'smoke','sessions':[record]}))
            self.assertEqual(select_sessions(td,'smoke',suite)[5],session.resolve())
            record['session_relative_directory']='experiment_5/session_fixture'
            (suite/'suite_summary.json').write_text(json.dumps({'mode':'smoke','sessions':[record]}))
            self.assertEqual(select_sessions(td,'smoke',suite)[5],session.resolve())
    @unittest.skipUnless(importlib.util.find_spec('matplotlib') and importlib.util.find_spec('pandas'),'pinned analyzer dependencies unavailable on host')
    def test_end_to_end_fixture_analysis_and_pending_missing(self):
        with tempfile.TemporaryDirectory() as td:
            def runner(cfg,mode,base,path,experiment_id,context):
                directory=Path(path)/'fixture'; result=run_fixture(directory,cfg)
                return result | {'result_directory':str(directory),'cleanup_errors':[]}
            cfg=load_phase5(overrides={'parallelisms':[4]}); cfg['run_config']['kafka_partitions']=2
            session,summary=execute(cfg,5,td,runner=runner); out=analyze(session)
            status=json.loads((out/'analysis.json').read_text())['charts']
            self.assertEqual(status['assigned_partition_count.png']['status'],'Generated')
            self.assertEqual(status['pending_records.png']['status'],'Not Executed')
            self.assertEqual((out/'report.md').read_text().count('## '),8)
            selected={5:session}; report=build_report(td,'smoke',selected,{5:out})
            self.assertEqual(json.loads((report/'report.json').read_text())['capacity_validation_status'],'Not Executed')
            self.assertTrue((out/'assignments.csv').exists())
    @unittest.skipUnless(importlib.util.find_spec('matplotlib'),'pinned analyzer dependencies unavailable on host')
    def test_actual_assignment_chart_is_independent_from_invalid_producer(self):
        with tempfile.TemporaryDirectory() as td:
            record={'run_id':'fixture','parallelism':2,'repetition':0,'execution_validity':'INVALID',
                'comparison_premises':'INCONCLUSIVE','assignment_status':'OBSERVED_COMPLETE'}
            diagnostic={'source_assignment':{'subtasks':[{'source_subtask':0,'assigned_partition_count':1},{'source_subtask':1,'assigned_partition_count':1}]}}
            charts=make_charts(Path(td),[record],{'fixture':diagnostic},[],'smoke')
            self.assertEqual(charts['throughput.png']['status'],'Not Executed')
            self.assertEqual(charts['assigned_partition_count.png']['status'],'Generated')


if __name__=='__main__': unittest.main()
