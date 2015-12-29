//
// adds socket-related knobs
//

hydra.Prms-testDescription += fcn "smoketest.scale.Fcns.describeSockets(${conserveSockets})" ncf;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};
