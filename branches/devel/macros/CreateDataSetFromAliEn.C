/* ========================================================================== *
 * CreateDataSetFromAliEn.C -- by Dario Berzano <dario.berzano@gmail.com>     *
 *                                                                            *
 * This macro is an interface to the afDataSetFromAliEn() function inside     *
 * the AFDSUtils package and allows the non-interactive creation of multiple  *
 * datasets directly from an AliEn find.                                      *
 *                                                                            *
 * Instructions: http://aaf.cern.ch/node/160                                  *
 *                                                                            *
 * Source code available on http://code.google.com/p/afdsmgrd                 *
 * ========================================================================== */

#include <TError.h>

void CreateDataSetFromAliEn(

  // Customize with your user and your group (optional)
  TString connStr   = "youruser:default@alice-caf.cern.ch",

  // Example for official real data
  TString basePath  = "/alice/data/2010/LHC10h",
  TString fileName  = "root_archive.zip",
  TString filter    = "ESDs_lowflux/pass1/.*<RUN>",  // beware: it's a regexp!
  TString anchor    = "AliESDs.root",
  TString treeName  = "/esdTree",
  TString runList   = "136837,137045",
  TString dsPattern = "LHC10h_<RUN>",

  // Example for official Monte Carlo
  /*
  TString basePath  = "/alice/sim/LHC10c9",
  TString fileName  = "root_archive.zip",
  TString filter    = ".*<RUN>/[0-9]{3,4}",  // beware: it's a regexp!
  TString anchor    = "AliESDs.root",
  TString treeName  = "/esdTree",
  TString runList   = "115315",
  TString dsPattern = "LHC10c9_<RUN>",
  */

  // Host name (and, optional, port) of redirector -- leave empty "" if it is
  // the same as the host name in connStr, but it differs if you're connecting
  // to AAF through a SSH tunnel
  TString redirHost = "",
  //TString redirHost = "alice-caf.cern.ch",
 
  // Possible options: setstaged:cache:verify:dryrun:aliencmd
  TString options   = "setstaged:dryrun:aliencmd"

  ) {

  TProof::Open(connStr, "masteronly");
  if (!gProof) {
    ::Error(gSystem->HostName(), "Problem connecting to PROOF, aborting");
    return;
  }

  if (gProof->EnablePackage("VO_ALICE@AFDSUtils::0.4.3")) {
    ::Error(gSystem->HostName(), "Can't enable AFDSUtils package, aborting");
    return;
  }

  TString afHost = connStr.Remove(0, connStr.Index('@')+1);
  {
    Ssiz_t colon = afHost.Index(':');
    if (colon > -1) afHost.Remove(colon, afHost.Length());
  }

  afSetDsPath("/pool/PROOF-AAF/proof/dataset");
  afSetProofUserHost(connStr.Data());
  afSetProofMode(1);
  afSetRedirUrl( Form("root://%s/$1",
    redirHost.IsNull() ? afHost.Data() : redirHost.Data()) );

  afPrintSettings();

  afDataSetFromAliEn(basePath, fileName, filter, anchor, treeName, runList,
    dsPattern, options);

}
