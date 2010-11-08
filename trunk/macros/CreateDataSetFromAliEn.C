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
  TString connStr   = "youruser:default@alice-caf.cern.ch",
  TString basePath  = "/alice/data/2010/LHC10e",
  TString fileName  = "root_archive.zip",
  TString anchor    = "AliESDs.root",
  TString treeName  = "/esdTree",
  TString runList   = "130831-130833,130803",
  Int_t   passNum   = 1, /* set as -1 if not relevant */
  TString dsPattern = "LHC10e_<RUN>_p<PASS>",
  TString options   = "setstaged:dryrun" /* setstaged:cache:verify:dryrun */
  ) {

  TProof::Open(connStr, "masteronly");
  if (!gProof) {
    ::Error(gSystem->HostName(), "Problem connecting to PROOF, aborting");
    return;
  }

  if (gProof->EnablePackage("VO_ALICE@AFDSUtils::0.4.0")) {
    ::Error(gSystem->HostName(), "Can't enable AFDSUtils package, aborting");
    return;
  }

  TString afHost = connStr.Remove(0, connStr.Index('@')+1);

  afSetDsPath("/pool/PROOF-AAF/proof/dataset");
  afSetProofUserHost(connStr.Data());
  afSetProofMode(1);
  afSetRedirUrl( Form("root://%s/$1", afHost.Data()) );

  afPrintSettings();

  afDataSetFromAliEn(basePath, fileName, anchor, treeName, runList, passNum,
    dsPattern, options);

}
