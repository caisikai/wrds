
from qlib.data.dataset.handler import DataHandlerLP
from qlib.data.dataset.processor import Processor
from qlib.utils import get_callable_kwargs
from qlib.data.dataset import processor as processor_module
from inspect import getfullargspec
from qlib.config import C

from typing import Callable, Union, Tuple, List, Iterator, Optional
from qlib.data.dataset.loader import DataLoader

def check_transform_proc(proc_l, fit_start_time, fit_end_time):
    new_l = []
    for p in proc_l:
        if not isinstance(p, Processor):
            klass, pkwargs = get_callable_kwargs(p, processor_module)
            args = getfullargspec(klass).args
            if "fit_start_time" in args and "fit_end_time" in args:
                assert (
                    fit_start_time is not None and fit_end_time is not None
                ), "Make sure `fit_start_time` and `fit_end_time` are not None."
                pkwargs.update(
                    {
                        "fit_start_time": fit_start_time,
                        "fit_end_time": fit_end_time,
                    }
                )
            proc_config = {"class": klass.__name__, "kwargs": pkwargs}
            if isinstance(p, dict) and "module_path" in p:
                proc_config["module_path"] = p["module_path"]
            new_l.append(proc_config)
        else:
            new_l.append(p)
    return new_l

#_DEFAUFLT_FUNDA_FIELDS = ["indfmt","datafmt","consol","popsrc","acctstd","acqmeth","bspr","compst","curcd","final","fyear","fyr","ismod","pddur","scf","src","stalt","upd","fdate","pdate","accli","acco","aco","acofs","acox","acoxfs","acqdisn","acqdiso","act","adpac","am","amdc","ao","aoloch","aox","ap","apalch","apch","apdpfs","apfs","apo","apofs","aqc","artfs","asdis","asinv","at","atoch","autxr","bcef","bct","ca","capcst","capfl","capr1","capr2","capr3","caprt","caps","capx","capxfi","ceq","cfbd","cfere","cflaoth","cfo","cfpdo","cga","ch","che","cheb","chech","chee","chefs","chenfd","chfs","chs","cmp","cogs","crvnli","cshr","cstk","custadv","dbtb","dbte","dc","dcsfd","dcufd","dd1","dd1fs","dfpac","dfxa","dispoch","dlc","dlcch","dlcfs","dltis","dltr","dltt","do","doc","dp","dpact","dpc","dpdc","dpltb","dpsc","dpstb","dptb","dptc","dptic","dv","dvc","dvp","dvpdp","dvrec","dvrre","dvsco","dvt","ea","ebit","ebitda","eiea","eieac","emp","eqdivp","ero","exre","exres","exreu","fatb","fate","fatl","fatp","fca","fdfr","fea","fel","ffs","fiao","fincf","fininc","finle","finre","finvao","fopo","fsrco","fsrcopo","fsrcopt","fsrct","fuseo","fuset","gdwl","iaeq","iafxi","ialoi","ialti","iamli","iaoi","iapli","iarei","iassi","iasti","iati","ib","ibc","ibki","ibmii","icapt","idiis","idilb","idilc","idis","idist","idit","idits","iire","initb","intan","intand","intanp","intc","intfact","intfl","intiact","intoact","intpd","intpn","intrc","invch","invdsp","invfg","invo","invrm","invsvc","invt","invtfs","invwip","iobd","ioi","iore","ip","ipti","isgr","isgt","isgu","isoth","ist","ivaco","ivaeq","ivao","ivch","ivgod","ivi","ivncf","ivpt","ivst","ivstch","ivstfs","lcabg","lcacl","lcacr","lcag","lcal","lcalt","lcam","lcao","lcast","lcat","lco","lcofs","lcox","lct","lctfs","lcuacu","liqresn","liqreso","lndep","lninc","lnmd","lnrep","lo","lse","lt","ltdch","ltdlch","ltlo","mib","mibn","mibt","mic","mii","miseq","mtl","ncfliq","neqmi","nio","nit","noasub","nopi","np","npanl","npaore","nparl","npat","npfs","oancf","oancfc","oancfd","oiadp","oibdp","onbalb","onbale","opprft","pacqp","pcl","pi","pliach","ppegt","ppent","prc","prodv","prosai","prstkc","prv","psfix","pstk","pstkn","pstkr","ptran","purtshr","pvon","pvt","radp","ragr","rari","rati","rawmsm","rcl","re","recch","recco","reccofs","rect","rectfs","rectr","rectrfs","revt","ris","rlri","rlt","rpag","rv","rvbci","rvbpi","rvbti","rveqt","rvlrv","rvri","rvsi","rvti","rvupi","rvutx","saa","sal","sale","sbdc","sc","sco","seq","shrcap","siv","spi","sppch","sppiv","ssnp","sstk","stbo","stfixa","stinv","stio","stkch","subdis","subpur","tdsg","tdst","teq","transa","tsca","tstk","tstlta","tx","txc","txdb","txdc","txdi","txditc","txo","txop","txp","txpd","txpfs","txt","txw","ui","unl","unnp","vpac","vpo","wcap","wcapch","wcapchc","wcapopc","wcaps","wcapsa","wcapsu","wcapt","wcapu","xacc","xaccfs","xago","xagt","xcom","xcomi","xdvre","xeqo","xi","xido","xidoc","xindb","xindc","xins","xinst","xint","xintd","xivi","xivre","xlr","xnitb","xobd","xoi","xopr","xopro","xore","xpp","xppfs","xpr","xrd","xrent","xs","xsga","xstf","xstfo","xstfws","xt","iid","exchg","isin","sedol","ajexi","curcdi","cshoi","cshpria","epsexcon","epsexnc","epsincon","epsinnc","icapi","nicon","ninc","pv","tstkni","conm","costat","fic","loc","naicsh","sich","rank","au","auop"]
#_DEFAUFLT_FUNDA_FIELDS = ['indfmt', 'datafmt', 'consol', 'popsrc', 'conm', 'costat',
#                            'loc', 'fic', 'fyr', 'curcd', 'fyear', 'upd', 'pddur',
#                            'src', 'acctstd', 'bspr', 'rank', 'auop', 'ib', 'pi', 'au', 'revt',
#                            'lse', 'at', 'icapt', 'ceq', 'iid', 'exchg', 'lt', 'final', 'conm', 'sedol','fdate']
#
_DEFAUFLT_FUNDA_FIELDS=['curcd', 'loc', 'acqmeth', 'compst', 'bspr', 'popsrc', 'auop', 'final', 'indfmt', 'consol', 'sedol', 'fdate', 'pdate', 'curcdi', 'acctstd', 'iid', 'fic', 'isin', 'stalt', 'datafmt', 'costat', 'au', 'conm']
class FundA(DataHandlerLP):
    def __init__(
        self,
        instruments="longest1k",
        start_time=None,
        end_time=None,
        freq="day",
        infer_processors=[],
        learn_processors=[],
        fit_start_time=None,
        fit_end_time=None,
        filter_pipe=None,
        inst_processor=None,
        **kwargs,
    ):
        infer_processors = check_transform_proc(infer_processors, fit_start_time, fit_end_time)
        learn_processors = check_transform_proc(learn_processors, fit_start_time, fit_end_time)

        data_loader = {
            "class": "WRDSDataLoader",
            "module_path": "data.wrds_dataloader",
            "kwargs": {
                "config": {
                    "feature": self.get_feature_config(),
                    "label": kwargs.get("label", self.get_label_config()),
                },
                "filter_pipe": filter_pipe,
                "freq": freq,
                "inst_processor": inst_processor,
            },
        }

        super().__init__(
            instruments=instruments,
            start_time=start_time,
            end_time=end_time,
            data_loader=data_loader,
            learn_processors=learn_processors,
            infer_processors=infer_processors,
        )

    def get_label_config(self):
        return (["$cstk"], ["LABEL0"])

    def get_feature_config(self):
        fields = []
        names = []

        for field in _DEFAUFLT_FUNDA_FIELDS:
            fields += ["$" + field]
            names += [field]

        return fields, names