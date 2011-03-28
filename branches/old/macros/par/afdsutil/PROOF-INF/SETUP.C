/**
 *  This PARfile contains the afdsutil.C macro which is loaded and built when
 *  needed only on the client.
 *
 *  by Dario Berzano <dario.berzano@gmail.com>
 */

void SETUP() {
  if ((gProof) && (gProof->TestBit(TProof::kIsClient))) {
    // We are on client
    gROOT->LoadMacro("afdsutil.C+");
    afdsutil();
  }
}
