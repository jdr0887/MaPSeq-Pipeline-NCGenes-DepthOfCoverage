package edu.unc.mapseq.pipeline.ncgenes.doc;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.renci.common.exec.BashExecutor;
import org.renci.common.exec.CommandInput;
import org.renci.common.exec.CommandOutput;
import org.renci.common.exec.Executor;
import org.renci.common.exec.ExecutorException;
import org.renci.jlrm.condor.CondorJob;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.model.EntityAttribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.HTSFSample;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.SequencerRun;
import edu.unc.mapseq.module.gatk.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk.GATKTableRecalibration;
import edu.unc.mapseq.pipeline.AbstractPipeline;
import edu.unc.mapseq.pipeline.PipelineException;
import edu.unc.mapseq.pipeline.PipelineJobFactory;
import edu.unc.mapseq.pipeline.PipelineUtil;

public class NCGenesDOCPipeline extends AbstractPipeline<NCGenesDOCPipelineBeanService> {

    private final Logger logger = LoggerFactory.getLogger(NCGenesDOCPipeline.class);

    private NCGenesDOCPipelineBeanService pipelineBeanService;

    public NCGenesDOCPipeline() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesDOCPipeline.class.getSimpleName().replace("Pipeline", "");
    }

    @Override
    public String getVersion() {
        Properties props = new Properties();
        try {
            props.load(this.getClass().getResourceAsStream("pipeline.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return props.getProperty("version", "0.0.1-SNAPSHOT");
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws PipelineException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;
        String intervalList = null;
        String prefix = null;
        String summaryCoverageThreshold = "1,2,5,8,10,15,20,30,50";

        if (getWorkflowPlan().getSequencerRun() == null && getWorkflowPlan().getHTSFSamples() == null) {
            logger.error("Don't have either sequencerRun and htsfSample");
            throw new PipelineException("Don't have either sequencerRun and htsfSample");
        }

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(this.pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample.getName(), getName()
                    .replace("DOC", ""));
            File tmpDir = new File(outputDirectory, "tmp");
            tmpDir.mkdirs();

            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                EntityAttribute attribute = attributeIter.next();
                if ("GATKDepthOfCoverage.intervalList".equals(attribute.getName())) {
                    intervalList = attribute.getValue();
                }
                if ("GATKDepthOfCoverage.prefix".equals(attribute.getName())) {
                    prefix = attribute.getValue();
                }
                if ("GATKDepthOfCoverage.summaryCoverageThreshold".equals(attribute.getName())) {
                    summaryCoverageThreshold = attribute.getValue();
                }
            }

            if (StringUtils.isEmpty(prefix)) {
                throw new PipelineException("prefix was empty");
            }

            if (StringUtils.isEmpty(intervalList)) {
                throw new PipelineException("intervalList was empty");
            }

            File intervalListFile = new File(intervalList);
            if (!intervalListFile.exists()) {
                throw new PipelineException("Interval list file does not exist: " + intervalListFile.getAbsolutePath());
            }

            Integer laneIndex = htsfSample.getLaneIndex();
            logger.debug("laneIndex = {}", laneIndex);
            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            File bamFile = null;

            List<File> potentialBAMFileList = PipelineUtil
                    .lookupFileByJobAndMimeType(fileDataSet, this.pipelineBeanService.getMaPSeqDAOBean(),
                            GATKTableRecalibration.class, MimeType.APPLICATION_BAM);

            // assume that only one GATKTableRecalibration job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new PipelineException("bam file to process was not found");
            }

            try {

                // new job
                CondorJob gatkGeneDepthOfCoverageJob = PipelineJobFactory.createJob(++count,
                        GATKDepthOfCoverageCLI.class, getWorkflowPlan(), htsfSample);
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.PHONEHOME,
                        GATKPhoneHomeType.NO_ET.toString());
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.DOWNSAMPLINGTYPE,
                        GATKDownsamplingType.NONE.toString());
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.INTERVALMERGING, "OVERLAPPING_ONLY");
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.REFERENCESEQUENCE,
                        getPipelineBeanService().getReferenceSequence());
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.VALIDATIONSTRICTNESS, "LENIENT");
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.OMITDEPTHOUTPUTATEACHBASE);

                if (summaryCoverageThreshold.contains(",")) {
                    for (String sct : StringUtils.split(summaryCoverageThreshold, ",")) {
                        gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.SUMMARYCOVERAGETHRESHOLD, sct);
                    }
                }

                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.INPUTFILE, bamFile.getAbsolutePath());
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.INTERVALS,
                        intervalListFile.getAbsolutePath());
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX,
                        bamFile.getName().replace(".bam", "." + prefix));
                graph.addVertex(gatkGeneDepthOfCoverageJob);

            } catch (Exception e) {
                throw new PipelineException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws PipelineException {

        File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
        Executor executor = BashExecutor.getInstance();

        if (getWorkflowPlan().getSequencerRun() == null && getWorkflowPlan().getHTSFSamples() == null) {
            logger.error("Don't have either sequencerRun and htsfSample");
            throw new PipelineException("Don't have either sequencerRun and htsfSample");
        }

        Set<HTSFSample> htsfSampleSet = new HashSet<HTSFSample>();

        if (getWorkflowPlan().getSequencerRun() != null) {
            logger.info("sequencerRun: {}", getWorkflowPlan().getSequencerRun().toString());
            try {
                htsfSampleSet.addAll(this.pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO()
                        .findBySequencerRunId(getWorkflowPlan().getSequencerRun().getId()));
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }
        }

        if (getWorkflowPlan().getHTSFSamples() != null) {
            logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample.getName(), getName()
                    .replace("DOC", ""));

            String prefix = null;
            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
            while (attributeIter.hasNext()) {
                EntityAttribute attribute = attributeIter.next();
                if ("GATKDepthOfCoverage.prefix".equals(attribute.getName())) {
                    prefix = attribute.getValue();
                }
            }

            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            File bamFile = null;

            List<File> potentialBAMFileList = PipelineUtil
                    .lookupFileByJobAndMimeType(fileDataSet, this.pipelineBeanService.getMaPSeqDAOBean(),
                            GATKTableRecalibration.class, MimeType.APPLICATION_BAM);

            // assume that only one GATKTableRecalibration job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new PipelineException("bam file to process was not found");
            }

            try {
                CommandInput commandInput = new CommandInput();
                commandInput.setCommand(String.format("/bin/cp %s/%s /tmp/", outputDirectory.getAbsolutePath(), bamFile
                        .getName().replace(".bam", prefix + ".*")));
                CommandOutput commandOutput = executor.execute(commandInput, mapseqrc);
                logger.info("commandOutput.getExitCode(): {}", commandOutput.getExitCode());
            } catch (ExecutorException e) {
                e.printStackTrace();
            }

        }

    }

    public NCGenesDOCPipelineBeanService getPipelineBeanService() {
        return pipelineBeanService;
    }

    public void setPipelineBeanService(NCGenesDOCPipelineBeanService pipelineBeanService) {
        this.pipelineBeanService = pipelineBeanService;
    }

}
