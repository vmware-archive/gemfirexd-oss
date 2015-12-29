//------------------------------------------------------------------------------
// Versioning
//
// Including this file makes it possible to run current tests against earlier
// releases or builds via a hydra system property in the batterytest file.
//
// This file can be included in the test configuration file, which requires
// the version to be set either in a default .prop file or in the batterytest
// file.  Or, it can be added to a local.conf for a given run, and the version
// set in the batterytest file.
//
// See https://wiki.gemstone.com/display/Hydra/Backwards+Compatibility+Testing
// for more information.
//------------------------------------------------------------------------------

hydra.ClientPrms-versionNames = version;

hydra.VersionPrms-names   = version;
hydra.VersionPrms-version = ${version};
