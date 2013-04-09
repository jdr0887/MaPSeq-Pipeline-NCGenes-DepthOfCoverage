package edu.unc.mapseq.pipeline.ncgenes.doc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ResourceBundle;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
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
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/pipeline/ncgenes/doc/pipeline");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
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
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

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
                gatkGeneDepthOfCoverageJob.addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX, prefix);
                graph.addVertex(gatkGeneDepthOfCoverageJob);

            } catch (Exception e) {
                throw new PipelineException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws PipelineException {
        logger.info("ENTERING postRun()");

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
            htsfSampleSet.addAll(getWorkflowPlan().getHTSFSamples());
        }

        logger.info("htsfSampleSet.size(): {}", htsfSampleSet.size());

        for (HTSFSample htsfSample : htsfSampleSet) {

            if ("Undetermined".equals(htsfSample.getBarcode())) {
                continue;
            }

            SequencerRun sequencerRun = htsfSample.getSequencerRun();
            File outputDirectory = createOutputDirectory(sequencerRun.getName(), htsfSample.getName(), getName()
                    .replace("DOC", ""));

            String prefix = null;
            Set<EntityAttribute> attributeSet = htsfSample.getAttributes();
            if (attributeSet == null) {
                attributeSet = new HashSet<EntityAttribute>();
            }
            if (attributeSet.size() > 0) {
                Iterator<EntityAttribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    EntityAttribute attribute = attributeIter.next();
                    if ("GATKDepthOfCoverage.prefix".equals(attribute.getName())) {
                        prefix = attribute.getValue();
                    }
                }
            }

            if (prefix == null) {
                logger.warn("GATKDepthOfCoverage.prefix doesn't exist");
                return;
            }

            logger.info("GATKDepthOfCoverage.prefix = {}", prefix);

            Set<FileData> fileDataSet = htsfSample.getFileDatas();

            Set<String> entityAttributeNameSet = new HashSet<String>();

            for (EntityAttribute attribute : attributeSet) {
                entityAttributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(entityAttributeNameSet);

            Collection<File> potentialFileList = FileUtils.listFiles(
                    outputDirectory,
                    FileFilterUtils.and(FileFilterUtils.prefixFileFilter(prefix),
                            FileFilterUtils.suffixFileFilter(".sample_summary")), null);

            if (potentialFileList != null && potentialFileList.size() > 0) {
                File docFile = potentialFileList.iterator().next();
                try {
                    List<String> lines = FileUtils.readLines(docFile);
                    if (lines != null) {
                        Iterator<String> lineIter = lines.iterator();
                        while (lineIter.hasNext()) {
                            String line = lineIter.next();
                            if (line.contains("Total")) {
                                String[] split = StringUtils.split(line);

                                String totalCoverageKey = String.format("GATKDepthOfCoverage.%s.totalCoverage", prefix);
                                if (synchSet.contains(totalCoverageKey)) {
                                    for (EntityAttribute attribute : attributeSet) {
                                        if (attribute.getName().equals(totalCoverageKey)) {
                                            attribute.setValue(split[1]);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new EntityAttribute(totalCoverageKey, split[1]));
                                }

                                String meanCoverageKey = String.format("GATKDepthOfCoverage.%s.mean", prefix);
                                if (synchSet.contains(meanCoverageKey)) {
                                    for (EntityAttribute attribute : attributeSet) {
                                        if (attribute.getName().equals(meanCoverageKey)) {
                                            attribute.setValue(split[1]);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new EntityAttribute(meanCoverageKey, split[2]));
                                }
                            }
                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            }

            potentialFileList = FileUtils.listFiles(
                    outputDirectory,
                    FileFilterUtils.and(FileFilterUtils.prefixFileFilter(prefix),
                            FileFilterUtils.suffixFileFilter(".sample_interval_summary")), null);

            Long totalPassedReads = null;
            for (EntityAttribute attribute : attributeSet) {
                if ("SAMToolsFlagstat.totalPassedReads".equals(attribute.getName())) {
                    totalPassedReads = Long.valueOf(attribute.getValue());
                    break;
                }
            }

            if (totalPassedReads == null) {
                logger.warn("SAMToolsFlagstat.totalPassedReads is null");
            }

            if (potentialFileList != null && potentialFileList.size() > 0) {
                try {
                    File docFile = potentialFileList.iterator().next();
                    BufferedReader br = new BufferedReader(new FileReader(docFile));
                    String line;
                    long totalCoverageCount = 0;
                    br.readLine();
                    while ((line = br.readLine()) != null) {
                        totalCoverageCount += Long.valueOf(StringUtils.split(line)[1].trim());
                    }
                    br.close();

                    logger.info("totalCoverageCount = {}", totalCoverageCount);

                    String totalCoverageCountKey = String.format(
                            "GATKDepthOfCoverage.%s.sample_interval_summary.totalCoverageCount", prefix);

                    if (synchSet.contains(totalCoverageCountKey)) {
                        for (EntityAttribute attribute : attributeSet) {
                            if (attribute.getName().equals(totalCoverageCountKey)) {
                                attribute.setValue(totalCoverageCount + "");
                                break;
                            }
                        }
                    } else {
                        attributeSet.add(new EntityAttribute(totalCoverageCountKey, totalCoverageCount + ""));
                    }

                    String numberOnTargetKey = String.format("%s.numberOnTarget", prefix);

                    if (totalPassedReads != null) {
                        if (synchSet.contains(numberOnTargetKey)) {
                            for (EntityAttribute attribute : attributeSet) {
                                if (attribute.getName().equals(numberOnTargetKey) && totalPassedReads != null) {
                                    attribute.setValue((double) totalCoverageCount / (totalPassedReads * 100) + "");
                                    break;
                                }
                            }
                        } else {
                            attributeSet.add(new EntityAttribute(numberOnTargetKey, (double) totalCoverageCount
                                    / (totalPassedReads * 100) + ""));
                        }
                    }
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                htsfSample.setAttributes(attributeSet);
                this.pipelineBeanService.getMaPSeqDAOBean().getHTSFSampleDAO().save(htsfSample);
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }

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
                commandInput.setCommand(String.format("/bin/cp %s/%s.* /tmp/", outputDirectory.getAbsolutePath(),
                        prefix));
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
