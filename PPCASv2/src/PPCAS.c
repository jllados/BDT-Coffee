#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>

float LOG_ZERO=-200000004008175468544.000000;
float LOG_UNDERFLOW_THRESHOLD = 7.50f;
float LOG_ONE = 0.0f;

#define NORM_F 1000
#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

double EXP (double x){
  if (x > -2){
    if (x > -0.5){
      if (x > 0)
	return exp(x);
      return (((0.03254409303190190000*x + 0.16280432765779600000)*x + 0.49929760485974900000)*x + 0.99995149601363700000)*x + 0.99999925508501600000;
    }
    if (x > -1)
      return (((0.01973899026052090000*x + 0.13822379685007000000)*x + 0.48056651562365000000)*x + 0.99326940370383500000)*x + 0.99906756856399500000;
    return (((0.00940528203591384000*x + 0.09414963667859410000)*x + 0.40825793595877300000)*x + 0.93933625499130400000)*x + 0.98369508190545300000;
  }
  if (x > -8){
    if (x > -4)
      return (((0.00217245711583303000*x + 0.03484829428350620000)*x + 0.22118199801337800000)*x + 0.67049462206469500000)*x + 0.83556950223398500000;
    return (((0.00012398771025456900*x + 0.00349155785951272000)*x + 0.03727721426017900000)*x + 0.17974997741536900000)*x + 0.33249299994217400000;
  }
  if (x > -16)
    return (((0.00000051741713416603*x + 0.00002721456879608080)*x + 0.00053418601865636800)*x + 0.00464101989351936000)*x + 0.01507447981459420000;
  return 0;
}

float LOOKUP (float x){
  if (x <= 1.00f) return ((-0.009350833524763f * x + 0.130659527668286f) * x + 0.498799810682272f) * x + 0.693203116424741f;
  if (x <= 2.50f) return ((-0.014532321752540f * x + 0.139942324101744f) * x + 0.495635523139337f) * x + 0.692140569840976f;
  if (x <= 4.50f) return ((-0.004605031767994f * x + 0.063427417320019f) * x + 0.695956496475118f) * x + 0.514272634594009f;

  return ((-0.000458661602210f * x + 0.009695946122598f) * x + 0.930734667215156f) * x + 0.168037164329057f;
}
void LOG_PLUS_EQUALS (float *x, float y){
  if (x[0] < y)
    x[0] = (x[0] == LOG_ZERO || y - x[0] >= LOG_UNDERFLOW_THRESHOLD) ? y : LOOKUP(y-x[0]) + x[0];
  else
    x[0] = (y == LOG_ZERO || x[0] - y >= LOG_UNDERFLOW_THRESHOLD) ? x[0]  : LOOKUP(x[0]-y) + y;
}

float LOG_ADD (float x, float y){
  if (x < y) return (x == LOG_ZERO || y - x >= LOG_UNDERFLOW_THRESHOLD) ? y : LOOKUP((y-x)) + x;
  return (y == LOG_ZERO || x - y >= LOG_UNDERFLOW_THRESHOLD) ? x : LOOKUP((x-y)) + y;
}

int ProbabilisticModel (int NumMatrixTypes, int NumInsertStates,float *initDistribMat,float *emitSingle,  float **emitPairs, float *gapOpen, float *gapExtend, float **transMat, float **insProb, float **matchProb, float *initialDistribution, float **transProb) {
    // build transition matrix
	int i, j;

	//Maybe an Issue with this topology
	transMat[0][0] = 1;
	for (i = 0; i < NumInsertStates; i++) {
		transMat[0][2*i+1] = gapOpen[2*i];
		transMat[0][2*i+2] = gapOpen[2*i+1];
		transMat[0][0] -= (gapOpen[2*i] + gapOpen[2*i+1]);
		transMat[2*i+1][2*i+1] = gapExtend[2*i];
		transMat[2*i+2][2*i+2] = gapExtend[2*i+1];
		transMat[2*i+1][2*i+2] = 0;
		transMat[2*i+2][2*i+1] = 0;
		transMat[2*i+1][0] = 1 - gapExtend[2*i];
		transMat[2*i+2][0] = 1 - gapExtend[2*i+1];
	}

	// create initial and transition probability matrices
	for (i = 0; i < NumMatrixTypes; i++) {
    	initialDistribution[i] = (float)log ((float)initDistribMat[i]);
    	for (j = 0; j < NumMatrixTypes; j++) {
			transProb[i][j] = (float)log ((float)transMat[i][j]);
		}
	}

	// create insertion and match probability matrices
	for (i = 0; i < 256; i++) {
		for (j = 0; j < NumMatrixTypes; j++) {
			insProb[i][j] = (float)log((float)emitSingle[i]);
		}
		for (j = 0; j < 256; j++) {
			matchProb[i][j] = (float)log((float)emitPairs[i][j]);
		}
	}
    
	return 1;
}

float ** declare_float(int N, int M) {
	int i;
	float **ptr = malloc(sizeof *ptr * N);
	if (ptr) {
	  for (i = 0; i < N; i++) {
		ptr[i] = malloc(sizeof *ptr[i] * M);
	  }
	}
	return ptr;
}

double ** declare_double(int N, int M) {
	int i;
	double **ptr = malloc(sizeof *ptr * N);
	if (ptr) {
	  for (i = 0; i < N; i++) {
		ptr[i] = malloc(sizeof *ptr[i] * M);
	  }
	}
	return ptr;
}

float *** declare_arrayN(int MAXX, int MAXY, int MAXZ) {
	int i, j;
	float ***ptr = malloc(sizeof *ptr * MAXX);
	if (ptr) {
		for(i=0;i<MAXX;i++) {
			ptr[i] = malloc(sizeof *ptr[i] * MAXY);
			if (ptr[i]) {
				for(j=0;j<MAXY;j++)
					ptr[i][j] = malloc(sizeof *ptr[i][j] * MAXZ);
			}
		}
    }
	return ptr;
}

void deallocate2D_float(float** arr2D,int rows) {
	int i;
	for(i=0;i<rows;i++) {
		free(arr2D[i]);
	}
	free(arr2D);
}

void deallocate2D_double(double** arr2D,int rows) {
	int i;
	for(i=0;i<rows;i++) {
		free(arr2D[i]);
	}
	free(arr2D);
}

void deallocate3D_float(float*** arr3D,int l,int m) {
	int i,j;
	for(i=0;i<l;i++) {
		for(j=0;j<m;j++) {
			free(arr3D[i][j]);
		}
		free(arr3D[i]);
	}
	free(arr3D);
}

int dirichlet_code( char aa) {
	char x;
	x=tolower (aa);
	
	if ( (x<'a') || (x>'z'))
		printf ( "CODE UNDEFINED\n");
	else if ( x<='a')
	    return x-'a';
	else if ( x<='i')
	    return x-('a'+1);
	else if ( x<= 'n')
	    return x-('a'+2);
	else if ( x<='t')
	    return x-('a'+3);
	else if ( x<='w')
	    return x-('a'+4);
	else if ( x=='y')
	    return x-('a'+5);
	else 
	  {
	    printf ("ERROR in dirichlet_code\n");
	    return 0;
	  }
	return 0;
}

int *dirichlet_code2aa_lu () {
	int *dm;
	char aal[21];
	int a;

	sprintf (aal, "acdefghiklmnpqrstvwy");
	dm=(int*)calloc (265, sizeof (int));
	memset (dm, -1, 20*sizeof(int));
	for (a=0; a<20; a++) {
		dm[dirichlet_code (aal[a])]=aal[a];
	}
	return dm;
}

int *aa2dirichlet_code_lu () {
	int *dm;
	char aal[21];
	int a;

	sprintf (aal, "acdefghiklmnpqrstvwy");
	dm=(int*)calloc (265, sizeof (int));
	memset (dm, -1, 265*sizeof(int));
	for (a=0; a<20; a++) {
		dm[aal[a]]=dm[(aal[a]-'a')+'A']=dm[(aal[a]-'a')]=dirichlet_code (aal[a]);
	}
	return dm;
}

double **aln2prf (char * seq, double **prf) {
	int *lu;
	int c,r,d;
	double tot;
	
	int slen = strlen(seq);

	lu=aa2dirichlet_code_lu();
	prf=declare_double (slen, 20);

	for (c=0; c<slen; c++) {
		tot = 0;
		memset (prf[c], 0, sizeof (double)*20);
		d=lu[seq[c]];
		if (d>=0) {
			prf[c][d]++;
			tot++;
		}
		if (tot>0) {
			for (r=0;r<20; r++) {
				prf[c][r]/=tot;
			}
		}
	}
	
	free(lu);
  	return prf;
}


int get_tot_prob2 (char * s1, char * s2, int nstates, float **matchProb, float **insProb, float *TmatchProb, float ***TinsProb) {
	int *lu, I, J, i, j, ij, k, r, r1, r2;
	double **prf1, **prf2;
	
	I = strlen(s1);
	J = strlen(s2);
	lu=dirichlet_code2aa_lu();
	prf1=aln2prf (s1, prf1);
	prf2=aln2prf (s2, prf2);
	
	  //get Ins for I
  for (i=1; i<=I; i++)
    {
      for (k=0; k<nstates; k++)
	{
	  TinsProb[0][k][i]=0;
	  for (r=0; r<20; r++)
	    {
	      TinsProb[0][k][i]+=(float)prf1[i-1][r]*insProb[lu[r]][k];
	    }
	}
    }
    
      //Get Ins for J
  for (j=1; j<=J; j++)
    {
      for (k=0; k<nstates; k++)
	{
	  TinsProb[1][k][j]=0;
	  for (r=0; r<20; r++)
	    {
	      TinsProb[1][k][j]+=(float)prf2[j-1][r]*insProb[lu[r]][k]; 
	    }
	}
    }
    
      for (ij=0,i=0; i<=I; i++)
    for (j=0; j<=J; j++, ij++)
      {
	float tot=0,f;
	if (i==0 || j==0)continue;
	TmatchProb[ij]=0;
	for (tot=0,r1=0; r1<20; r1++)
	  {
	    for (r2=0; r2<20; r2++)
	      {
		f=(float)prf1[i-1][r1]*(float)prf2[j-1][r2];
		TmatchProb[ij]+=matchProb[lu[r1]][lu[r2]]*f;
	      }
	  }
      }
      
	free(lu);
    deallocate2D_double(prf1,I);
    deallocate2D_double(prf2,J);
    
	return 1;
}

float * forward_proba_pair_wise (int seq1Length, int seq2Length, int NumMatrixTypes, int NumInsertStates, float **transMat, float *initialDistribution,float *matchProb, float ***insProb, float **transProb) {
	float *forward;
	int l, a, k, i, j, ij, i1j1, i1j, ij1, m;
	l=(seq1Length+1)*(seq2Length+1)*NumMatrixTypes;
	
	forward=(float*)calloc (l, sizeof (float));
    
    for (a=0; a<l; a++)forward[a]=LOG_ZERO;
    
    forward[0 + NumMatrixTypes * (1 * (seq2Length+1) + 1)] = initialDistribution[0] + matchProb[seq2Length+2];

  for (k = 0; k < NumInsertStates; k++)
    {
      forward[2*k+1 + NumMatrixTypes * (1 * (seq2Length+1) + 0)] = initialDistribution[2*k+1] + insProb[0][k][1];
      forward[2*k+2 + NumMatrixTypes * (0 * (seq2Length+1) + 1)] = initialDistribution[2*k+2] + insProb[1][k][1];
    }
    
    // remember offset for each index combination
    ij = 0;
    i1j = -seq2Length - 1;
    ij1 = -1;
    i1j1 = -seq2Length - 2;

    ij *= NumMatrixTypes;
    i1j *= NumMatrixTypes;
    ij1 *= NumMatrixTypes;
    i1j1 *= NumMatrixTypes;
    
     // compute forward scores
    for (m=0,i = 0; i <= seq1Length; i++)
      {
	for (j = 0; j <= seq2Length; j++, m++)
	  {
	  if (i > 1 || j > 1)
	    {
	    if (i > 0 && j > 0)
	      {
		//Sum over all possible alignments
		forward[0 + ij] = forward[0 + i1j1] + transProb[0][0];
		for (k = 1; k < NumMatrixTypes; k++)
		  {
		    LOG_PLUS_EQUALS (&forward[0 + ij], forward[k + i1j1] + transProb[k][0]);
		  }
		forward[0 + ij] += matchProb[m];
	      }
	    if ( i > 0)
	      {
	      for (k = 0; k < NumInsertStates; k++)
		{
		  forward[2*k+1 + ij] = insProb[0][k][i] + LOG_ADD (forward[0 + i1j] + transProb[0][2*k+1],forward[2*k+1 + i1j] + transProb[2*k+1][2*k+1]);
		}
	      }
	  if (j > 0)
	    {
	    for (k = 0; k < NumInsertStates; k++)
	      {
		forward[2*k+2 + ij] = insProb[1][k][j] +LOG_ADD (forward[0 + ij1] + transProb[0][2*k+2],forward[2*k+2 + ij1] + transProb[2*k+2][2*k+2]);
	      }
	    }
	  }

        ij += NumMatrixTypes;
        i1j += NumMatrixTypes;
        ij1 += NumMatrixTypes;
        i1j1 += NumMatrixTypes;
      }
    }
	
	return forward;
}

float * backward_proba_pair_wise ( int seq1Length, int seq2Length, int NumMatrixTypes, int NumInsertStates, float **transMat, float *initialDistribution,float *matchProb, float ***insProb, float **transProb) {
	float *backward;
	int l, a, k, i, j, ij, i1j1, i1j, ij1, m;
	
	l=(seq1Length+1)*(seq2Length+1)*NumMatrixTypes;
	
    backward=(float*)calloc (l, sizeof (float));
    
  for (a=0; a<l; a++)backward[a]=LOG_ZERO;

  for (k = 0; k < NumMatrixTypes; k++) {
    backward[NumMatrixTypes * ((seq1Length+1) * (seq2Length+1) - 1) + k] = initialDistribution[k];
  }
 
  //Difference with Probcons: this emission is not added to the bward
  backward[NumMatrixTypes * ((seq1Length+1) * (seq2Length+1) - 1) + 0]+=matchProb[(seq1Length+1) * (seq2Length+1) - 1];
  // remember offset for each index combination
  ij = (seq1Length+1) * (seq2Length+1) - 1;

  i1j = ij + seq2Length + 1;
  ij1 = ij + 1;
  i1j1 = ij + seq2Length + 2;
  ij *= NumMatrixTypes;
  i1j *= NumMatrixTypes;
  ij1 *= NumMatrixTypes;
  i1j1 *= NumMatrixTypes;
  
    // compute backward scores
  for (i = seq1Length; i >= 0; i--)
    {
      for (j = seq2Length; j >= 0; j--)
	{
	  if (i < seq1Length && j < seq2Length)
	    {
	      m=((i+1)*(seq2Length+1))+j+1;//The backward and the forward are offset by 1
	      float ProbXY = backward[0 + i1j1] + matchProb[m];

	      for (k = 0; k < NumMatrixTypes; k++)
		{
		  LOG_PLUS_EQUALS (&backward[k + ij], ProbXY + transProb[k][0]);
		}
	    }
	  if (i < seq1Length)
	    {
	      for (k = 0; k < NumInsertStates; k++)
		{
		LOG_PLUS_EQUALS (&backward[0 + ij], backward[2*k+1 + i1j] + insProb[0][k][i+1] + transProb[0][2*k+1]);
		LOG_PLUS_EQUALS (&backward[2*k+1 + ij], backward[2*k+1 + i1j] + insProb[0][k][i+1] + transProb[2*k+1][2*k+1]);
		}
	    }
        if (j < seq2Length)
	  {
	    for (k = 0; k < NumInsertStates; k++)
	      {
		//+1 because the backward and the forward are offset by 1
		LOG_PLUS_EQUALS (&backward[0 + ij], backward[2*k+2 + ij1] + insProb[1][k][j+1] + transProb[0][2*k+2]);
		LOG_PLUS_EQUALS (&backward[2*k+2 + ij], backward[2*k+2 + ij1] + insProb[1][k][j+1] + transProb[2*k+2][2*k+2]);
	      }
	  }
        ij -= NumMatrixTypes;
        i1j -= NumMatrixTypes;
        ij1 -= NumMatrixTypes;
        i1j1 -= NumMatrixTypes;
	}
    }
	
	return backward;
}

float ComputeTotalProbability (int seq1Length, int seq2Length,int NumMatrixTypes, int NumInsertStates,float *forward, float *backward) {
    float totalForwardProb = LOG_ZERO;
    float totalBackwardProb = LOG_ZERO;
    int k;

    for (k = 0; k < NumMatrixTypes; k++)
      {
      LOG_PLUS_EQUALS (&totalForwardProb,forward[k + NumMatrixTypes * ((seq1Length+1) * (seq2Length+1) - 1)] + backward[k + NumMatrixTypes * ((seq1Length+1) * (seq2Length+1) - 1)]);
      }

    totalBackwardProb =forward[0 + NumMatrixTypes * (1 * (seq2Length+1) + 1)] +backward[0 + NumMatrixTypes * (1 * (seq2Length+1) + 1)];

    for (k = 0; k < NumInsertStates; k++)
      {
      LOG_PLUS_EQUALS (&totalBackwardProb,forward[2*k+1 + NumMatrixTypes * (1 * (seq2Length+1) + 0)] +backward[2*k+1 + NumMatrixTypes * (1 * (seq2Length+1) + 0)]);
      LOG_PLUS_EQUALS (&totalBackwardProb,forward[2*k+2 + NumMatrixTypes * (0 * (seq2Length+1) + 1)] +backward[2*k+2 + NumMatrixTypes * (0 * (seq2Length+1) + 1)]);
      }
    
    return (totalForwardProb + totalBackwardProb) / 2;
}

int ProbaMatrix2CL (int I, int J, int NumMatrixTypes, int NumInsertStates, float *forward, float *backward, float thr, int *lib) {
	float totalProb;
	totalProb = ComputeTotalProbability (I,J,NumMatrixTypes, NumInsertStates,forward, backward);
	
	int list_n, i, j, ij;
	double v;
	ij = 0;
	
  for (list_n=0,ij=0,i =0; i <= I; i++)
    {
      for (j =0; j <= J; j++, ij+=NumMatrixTypes)
	{
	  v= EXP (MIN(LOG_ONE,(forward[ij] + backward[ij] - totalProb)));

	  if (v>thr)//Conservative reduction of the list size to speed up the sorting
	    {
		  lib[(list_n*3)]=i;
	      lib[(list_n*3)+1]=j;
	      lib[(list_n*3)+2]=(int)((float)v*(float)NORM_F);
	      list_n++;
	    }
	}
    }
	
	return list_n;
}

int proba_pair_wise(char * s1, char * s2, int *lib) {
	int NumInsertStates = 2;
	int NumMatrixTypes = 5;
	float initDistribMat[] = { 0.6814756989f, 8.615339902e-05f, 8.615339902e-05f, 0.1591759622f, 0.1591759622f };
	float gapOpen[] = { 0.0119511066f, 0.0119511066f, 0.008008334786f, 0.008008334786f };
	float gapExtend[] = { 0.3965826333f, 0.3965826333f, 0.8988758326f, 0.8988758326f };
	char alphabetDefault[] = "ARNDCQEGHILKMFPSTWYV";
	float emitSingleDefault[20] = {
  0.07831005f, 0.05246024f, 0.04433257f, 0.05130349f, 0.02189704f,
  0.03585766f, 0.05615771f, 0.07783433f, 0.02601093f, 0.06511648f,
  0.09716489f, 0.05877077f, 0.02438117f, 0.04463228f, 0.03940142f,
  0.05849916f, 0.05115306f, 0.01203523f, 0.03124726f, 0.07343426f
};
	float emitPairsDefault[20][20] = {
  {0.02373072f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00244502f, 0.01775118f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00210228f, 0.00207782f, 0.01281864f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00223549f, 0.00161657f, 0.00353540f, 0.01911178f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00145515f, 0.00044701f, 0.00042479f, 0.00036798f, 0.01013470f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00219102f, 0.00253532f, 0.00158223f, 0.00176784f, 0.00032102f, 0.00756604f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00332218f, 0.00268865f, 0.00224738f, 0.00496800f, 0.00037956f, 0.00345128f, 0.01676565f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00597898f, 0.00194865f, 0.00288882f, 0.00235249f, 0.00071206f, 0.00142432f, 0.00214860f, 0.04062876f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00114353f, 0.00132105f, 0.00141205f, 0.00097077f, 0.00026421f, 0.00113901f, 0.00131767f, 0.00103704f, 0.00867996f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00318853f, 0.00138145f, 0.00104273f, 0.00105355f, 0.00094040f, 0.00100883f, 0.00124207f, 0.00142520f, 0.00059716f, 0.01778263f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00449576f, 0.00246811f, 0.00160275f, 0.00161966f, 0.00138494f, 0.00180553f, 0.00222063f, 0.00212853f, 0.00111754f, 0.01071834f, 0.03583921f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00331693f, 0.00595650f, 0.00257310f, 0.00252518f, 0.00046951f, 0.00312308f, 0.00428420f, 0.00259311f, 0.00121376f, 0.00157852f, 0.00259626f, 0.01612228f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00148878f, 0.00076734f, 0.00063401f, 0.00047808f, 0.00037421f, 0.00075546f, 0.00076105f, 0.00066504f, 0.00042237f, 0.00224097f, 0.00461939f, 0.00096120f, 0.00409522f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00165004f, 0.00090768f, 0.00084658f, 0.00069041f, 0.00052274f, 0.00059248f, 0.00078814f, 0.00115204f, 0.00072545f, 0.00279948f, 0.00533369f, 0.00087222f, 0.00116111f, 0.01661038f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00230618f, 0.00106268f, 0.00100282f, 0.00125381f, 0.00034766f, 0.00090111f, 0.00151550f, 0.00155601f, 0.00049078f, 0.00103767f, 0.00157310f, 0.00154836f, 0.00046718f, 0.00060701f, 0.01846071f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00631752f, 0.00224540f, 0.00301397f, 0.00285226f, 0.00094867f, 0.00191155f, 0.00293898f, 0.00381962f, 0.00116422f, 0.00173565f, 0.00250962f, 0.00312633f, 0.00087787f, 0.00119036f, 0.00180037f, 0.01346609f, 0.0f, 0.0f, 0.0f, 0.0f},
  {0.00389995f, 0.00186053f, 0.00220144f, 0.00180488f, 0.00073798f, 0.00154526f, 0.00216760f, 0.00214841f, 0.00077747f, 0.00248968f, 0.00302273f, 0.00250862f, 0.00093371f, 0.00107595f, 0.00147982f, 0.00487295f, 0.01299436f, 0.0f, 0.0f, 0.0f},
  {0.00039119f, 0.00029139f, 0.00021006f, 0.00016015f, 0.00010666f, 0.00020592f, 0.00023815f, 0.00038786f, 0.00019097f, 0.00039549f, 0.00076736f, 0.00028448f, 0.00016253f, 0.00085751f, 0.00015674f, 0.00026525f, 0.00024961f, 0.00563625f, 0.0f, 0.0f},
  {0.00131840f, 0.00099430f, 0.00074960f, 0.00066005f, 0.00036626f, 0.00070192f, 0.00092548f, 0.00089301f, 0.00131038f, 0.00127857f, 0.00219713f, 0.00100817f, 0.00054105f, 0.00368739f, 0.00047608f, 0.00102648f, 0.00094759f, 0.00069226f, 0.00999315f, 0.0f},
  {0.00533241f, 0.00169359f, 0.00136609f, 0.00127915f, 0.00119152f, 0.00132844f, 0.00178697f, 0.00194579f, 0.00071553f, 0.01117956f, 0.00914460f, 0.00210897f, 0.00197461f, 0.00256159f, 0.00135781f, 0.00241601f, 0.00343452f, 0.00038538f, 0.00148001f, 0.02075171f}
};
	float thr=0.01;//ProbCons Default
	int I = strlen(s1);
	int J = strlen(s2);

	int l, i, j;
	float *emitSingle, *s, **emitPairs, **p;
	
	l=strlen(alphabetDefault);
	p=declare_float (l,l);
	emitPairs=declare_float (256, 256);
	
	s=(float*)calloc (l, sizeof (float));
	emitSingle=(float*)calloc (256, sizeof (float));
	
	for (i=0; i<l; i++) {
		s[i]=emitSingleDefault[i];
		for (j=0; j<l; j++) {
			p[i][j]=emitPairsDefault[i][j];
		}
	}
		
	for (i=0; i<256; i++) {
	   emitSingle[i]=1;
	   for (j=0; j<256; j++) {
	     emitPairs[i][j]=1;
	   }
	}
	
	for (i=0; i<l; i++) {
	   int C1,c1, C2,c2;
	   c1=tolower(alphabetDefault[i]);
	   C1=toupper(alphabetDefault[i]);
	   emitSingle[c1]=s[i];
	   emitSingle[C1]=s[i];
	   for (j=0; j<=i; j++) {
	       c2=tolower(alphabetDefault[j]);
	       C2=toupper(alphabetDefault[j]);
	       emitPairs[c1][c2]=p[i][j];
	       emitPairs[C1][c2]=p[i][j];
	       emitPairs[C1][C2]=p[i][j];
	       emitPairs[c1][C2]=p[i][j];
	       emitPairs[c2][c1]=p[i][j];
	       emitPairs[C2][c1]=p[i][j];
	       emitPairs[C2][C1]=p[i][j];
	       emitPairs[c2][C1]=p[i][j];
	   }
	}
    
    float **transMat, **insProb, **matchProb, *initialDistribution, **transProb;

    transMat=declare_float (2*NumInsertStates+1, 2*NumInsertStates+1);
    transProb=declare_float (2*NumInsertStates+1,2* NumInsertStates+1);
    insProb=declare_float (256,NumMatrixTypes);
	matchProb=declare_float (256, 256);
    initialDistribution=(float*)calloc (2*NumMatrixTypes+1, sizeof (float));
    
    ProbabilisticModel (NumMatrixTypes, NumInsertStates, initDistribMat, emitSingle, emitPairs, gapOpen, gapExtend, transMat, insProb, matchProb, initialDistribution, transProb);
    
	float ***TinsProb, *TmatchProb;
	
	l=(I+1)*(J+1);
	TmatchProb=(float*)calloc ( l, sizeof (float));
	l=MAX(I,J)+1;
	TinsProb=declare_arrayN (2,NumMatrixTypes,l);
	
	float *F, *B;
	
	get_tot_prob2 (s1, s2, NumMatrixTypes, matchProb, insProb, TmatchProb, TinsProb);
	
	F=forward_proba_pair_wise (I, J, NumMatrixTypes, NumInsertStates,transMat, initialDistribution,TmatchProb,TinsProb, transProb);
   	B=backward_proba_pair_wise (I, J, NumMatrixTypes, NumInsertStates,transMat, initialDistribution,TmatchProb,TinsProb, transProb);
   	int list_n = ProbaMatrix2CL(I, J, NumMatrixTypes, NumInsertStates, F, B, thr, lib);
	
	//single malloc
	free(emitSingle);
	free(s);
	free(initialDistribution);
	free(TmatchProb);
	free(F);
	free(B);
	//2d malloc
	deallocate2D_float(emitPairs,256);
	deallocate2D_float(p,strlen(alphabetDefault));
	deallocate2D_float(transMat,2*NumInsertStates+1);
	deallocate2D_float(transProb,2*NumInsertStates+1);
	deallocate2D_float(insProb,256);
	deallocate2D_float(matchProb,256);
	//3d malloc
	deallocate3D_float(TinsProb,2,NumMatrixTypes);
	
	return list_n;
}


