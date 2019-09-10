package org.broadinstitute.hellbender.tools.walkers.annotator;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFFormatHeaderLine;
import htsjdk.variant.vcf.VCFStandardHeaderLines;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.genotyper.AlleleLikelihoods;
import org.broadinstitute.hellbender.utils.help.HelpConstants;
import org.broadinstitute.hellbender.utils.logging.OneShotLogger;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Depth of informative coverage for each sample.
 *
 * <p>This annotation is similar to the sample-level DP annotation, which counts read depth after general filtering, but with an extra layer of stringency. Its purpose is to provide the count of reads that are actually considered informative by HaplotypeCaller (HC), using pre-read likelihoods that are produced internally by HC.</p>
 * <p>In this context, an informative read is defined as one that allows the allele it carries to be easily distinguished. In contrast, a read might be considered uninformative if, for example, it only partially overlaps a short tandem repeat and it is not clear whether the read contains the reference allele or an extra repeat.</p>
 *
 * <p>See the method documentation on <a href="http://www.broadinstitute.org/gatk/guide/article?id=4721">using coverage information</a> for important interpretation details.</p>
 *
 * <h3>Caveats</h3>
 * <ul>
 *     <li>This annotation can only be generated by HaplotypeCaller (it will not work when called from VariantAnnotator).</li>
 * </ul>
 *
 * <h3>Related annotations</h3>
 * <ul>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_DepthPerAlleleBySample.php">DepthPerAlleleBySample</a></b> calculates depth of coverage for each allele per sample (AD).</li>
 *     <li><b><a href="https://www.broadinstitute.org/gatk/guide/tooldocs/org_broadinstitute_gatk_tools_walkers_annotator_Coverage.php">Coverage</a></b> gives the filtered depth of coverage for each sample and the unfiltered depth across all samples.</li>
 * </ul>
 *
 */
@DocumentedFeature(groupName=HelpConstants.DOC_CAT_ANNOTATORS, groupSummary=HelpConstants.DOC_CAT_ANNOTATORS_SUMMARY, summary="Depth of informative coverage for each sample (DP)")
public final class DepthPerSampleHC extends GenotypeAnnotation implements StandardHCAnnotation, StandardMutectAnnotation {
    private final transient OneShotLogger logger = new OneShotLogger(DepthPerSampleHC.class);

    @Override
    public void annotate( final ReferenceContext ref,
                          final VariantContext vc,
                          final Genotype g,
                          final GenotypeBuilder gb,
                          final AlleleLikelihoods<GATKRead, Allele> likelihoods ) {
        Utils.nonNull(vc);
        Utils.nonNull(g);
        Utils.nonNull(gb);

        if ( likelihoods == null || !g.isCalled() ) {
            logger.warn("Annotation will not be calculated, genotype is not called or alleleLikelihoodMap is null");
            return;
        }

        // check that there are reads
        final String sample = g.getSampleName();
        if (likelihoods.sampleEvidenceCount(likelihoods.indexOfSample(sample)) == 0) {
            gb.DP(0);
            return;
        }

        final Set<Allele> alleles = new LinkedHashSet<>(vc.getAlleles());

        // make sure that there's a meaningful relationship between the alleles in the likelihoods and our VariantContext
        if ( !likelihoods.alleles().containsAll(alleles) ) {
            logger.warn("VC alleles " + alleles + " not a strict subset of AlleleLikelihoods alleles " + likelihoods.alleles());
            return;
        }

        // the depth for the HC is the sum of the informative alleles at this site.  It's not perfect (as we cannot
        // differentiate between reads that align over the event but aren't informative vs. those that aren't even
        // close) but it's a pretty good proxy and it matches with the AD field (i.e., sum(AD) = DP).
        final Map<Allele, List<Allele>> alleleSubset = alleles.stream().collect(Collectors.toMap(a -> a, a -> Arrays.asList(a)));
        final AlleleLikelihoods<GATKRead, Allele> subsettedLikelihoods = likelihoods.marginalize(alleleSubset);
        final int depth = (int) subsettedLikelihoods.bestAllelesBreakingTies(sample).stream().filter(ba -> ba.isInformative()).count();
        gb.DP(depth);
    }

    @Override
    public List<String> getKeyNames() {
        return Collections.singletonList(VCFConstants.DEPTH_KEY);
    }

    @Override
    public List<VCFFormatHeaderLine> getDescriptions() {
        return Collections.singletonList(VCFStandardHeaderLines.getFormatLine(VCFConstants.DEPTH_KEY));
    }
}
