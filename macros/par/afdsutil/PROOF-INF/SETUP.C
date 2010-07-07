/**
 *  Special PARFile to always download the latest version of the afdsutil.C
 *  macro
 *
 *  by Dario Berzano <dario.berzano@gmail.com>
 */

void SETUP() {
  if ((gProof) && (gProof->TestBit(TProof::kIsClient))) {
    // We are on client
    gROOT->LoadMacro("/tmp/afdsutil.C+");
    afdsutil();
  }
}
