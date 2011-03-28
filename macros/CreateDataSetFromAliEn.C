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

  // Example for ESDs from official real data: the <RUN9> string in basePath
  // means to insert the run number, zero-padded to 9 digits. In dsPattern,
  // <RUN> is zero-padded to 9 digits by default. Keep in mind that basePath and
  // fileName are in fact the first two parameters of AliEn find, thus they
  // support basic jolly characters expansion features. The '*' is used as jolly
  // character
  TString basePath  = "/alice/data/2010/LHC10h/<RUN9>/ESDs_lowflux/pass1/*.*",
  TString fileName  = "root_archive.zip",
  TString filter    = "",  // no filter after AliEn find is used
  TString anchor    = "AliESDs.root",
  TString treeName  = "/esdTree",
  TString runList   = "136837,137045",
  TString dsPattern = "LHC10h_<RUN>_ESDs_lowflux",

  // Example for all AODs from official real data: see above for an explanation
  // on format strings. This example makes use of a more complex regular
  // expression to filter all the entries under basePath which contain in their
  // name a directory "AOD" followed by three numbers and a subdirectory whose
  // name is a number with 3 or 4 digits. This example also shows how to specify
  // a run range
  /*
  TString basePath  = "/alice/data/2010/LHC10h/<RUN9>/ESDs/pass1",
  TString fileName  = "root_archive.zip",
  TString filter    = "AOD[0-9]{3}/[0-9]{3,4}",  // get only AODnnn/mmmm dirs
  TString anchor    = "AliAOD.root",  // *not* AliAODs.root!
  TString treeName  = "/aodTree",
  TString runList   = "139104-139107,139306",
  TString dsPattern = "LHC10h_<RUN>_AllAODs",
  */

  // Example for ESDs from official Monte Carlo. In this example, filter is used
  // to prune AOD and QA directories (just try AliEn find with and without
  // that), and run number is zero-padded to 6 digits
  /*
  TString basePath  = "/alice/sim/LHC10c9/<RUN6>",
  TString fileName  = "root_archive.zip",
  TString filter    = "<RUN>/[0-9]{3,4}",  // beware: extended regexp!
  TString anchor    = "AliESDs.root",
  TString treeName  = "/esdTree",
  TString runList   = "115315,119846",
  TString dsPattern = "MC_LHC10c9_<RUN>_ESDs",
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
