package edu.unc.mapseq.workflow.ncgenes.doc;

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
import org.renci.jlrm.condor.CondorJobBuilder;
import org.renci.jlrm.condor.CondorJobEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.unc.mapseq.dao.MaPSeqDAOException;
import edu.unc.mapseq.dao.SampleDAO;
import edu.unc.mapseq.dao.model.Attribute;
import edu.unc.mapseq.dao.model.FileData;
import edu.unc.mapseq.dao.model.MimeType;
import edu.unc.mapseq.dao.model.Sample;
import edu.unc.mapseq.dao.model.Workflow;
import edu.unc.mapseq.dao.model.WorkflowRun;
import edu.unc.mapseq.dao.model.WorkflowRunAttempt;
import edu.unc.mapseq.module.gatk.GATKDepthOfCoverageCLI;
import edu.unc.mapseq.module.gatk.GATKDownsamplingType;
import edu.unc.mapseq.module.gatk.GATKPhoneHomeType;
import edu.unc.mapseq.module.gatk.GATKTableRecalibration;
import edu.unc.mapseq.workflow.WorkflowException;
import edu.unc.mapseq.workflow.WorkflowUtil;
import edu.unc.mapseq.workflow.impl.AbstractSampleWorkflow;
import edu.unc.mapseq.workflow.impl.WorkflowJobFactory;

public class NCGenesDOCWorkflow extends AbstractSampleWorkflow {

    private final Logger logger = LoggerFactory.getLogger(NCGenesDOCWorkflow.class);

    public NCGenesDOCWorkflow() {
        super();
    }

    @Override
    public String getName() {
        return NCGenesDOCWorkflow.class.getSimpleName().replace("Workflow", "");
    }

    @Override
    public String getVersion() {
        ResourceBundle bundle = ResourceBundle.getBundle("edu/unc/mapseq/workflow/ncgenes/doc/workflow");
        String version = bundle.getString("version");
        return StringUtils.isNotEmpty(version) ? version : "0.0.1-SNAPSHOT";
    }

    @Override
    public Graph<CondorJob, CondorJobEdge> createGraph() throws WorkflowException {
        logger.debug("ENTERING createGraph()");

        DirectedGraph<CondorJob, CondorJobEdge> graph = new DefaultDirectedGraph<CondorJob, CondorJobEdge>(
                CondorJobEdge.class);

        int count = 0;
        String intervalList = null;
        String prefix = null;
        String summaryCoverageThreshold = "1,2,5,8,10,15,20,30,50";

        Set<Sample> sampleSet = getAggregatedSamples();
        logger.info("sampleSet.size(): {}", sampleSet.size());

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        String siteName = getWorkflowBeanService().getAttributes().get("siteName");
        String referenceSequence = getWorkflowBeanService().getAttributes().get("referenceSequence");

        WorkflowRunAttempt attempt = getWorkflowRunAttempt();
        WorkflowRun workflowRun = attempt.getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            File outputDirectory = new File(sample.getOutputDirectory(), getName().replace("DOC", ""));
            File tmpDirectory = new File(outputDirectory, "tmp");
            tmpDirectory.mkdirs();

            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
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
            }

            if (StringUtils.isEmpty(prefix)) {
                throw new WorkflowException("prefix was empty");
            }

            if (StringUtils.isEmpty(intervalList)) {
                throw new WorkflowException("intervalList was empty");
            }

            File intervalListFile = new File(intervalList);
            if (!intervalListFile.exists()) {
                throw new WorkflowException("Interval list file does not exist: " + intervalListFile.getAbsolutePath());
            }

            Integer laneIndex = sample.getLaneIndex();
            logger.debug("laneIndex = {}", laneIndex);
            Set<FileData> fileDataSet = sample.getFileDatas();

            File bamFile = null;

            List<File> potentialBAMFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getWorkflowBeanService().getMaPSeqDAOBean(), GATKTableRecalibration.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one GATKTableRecalibration job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
            }

            try {

                // new job
                CondorJobBuilder builder = WorkflowJobFactory
                        .createJob(++count, GATKDepthOfCoverageCLI.class, attempt, sample).siteName(siteName)
                        .initialDirectory(outputDirectory.getAbsolutePath());
                builder.addArgument(GATKDepthOfCoverageCLI.PHONEHOME, GATKPhoneHomeType.NO_ET.toString())
                        .addArgument(GATKDepthOfCoverageCLI.DOWNSAMPLINGTYPE, GATKDownsamplingType.NONE.toString())
                        .addArgument(GATKDepthOfCoverageCLI.INTERVALMERGING, "OVERLAPPING_ONLY")
                        .addArgument(GATKDepthOfCoverageCLI.REFERENCESEQUENCE, referenceSequence)
                        .addArgument(GATKDepthOfCoverageCLI.VALIDATIONSTRICTNESS, "LENIENT")
                        .addArgument(GATKDepthOfCoverageCLI.OMITDEPTHOUTPUTATEACHBASE)
                        .addArgument(GATKDepthOfCoverageCLI.INPUTFILE, bamFile.getAbsolutePath())
                        .addArgument(GATKDepthOfCoverageCLI.INTERVALS, intervalListFile.getAbsolutePath())
                        .addArgument(GATKDepthOfCoverageCLI.OUTPUTPREFIX, prefix);

                if (summaryCoverageThreshold.contains(",")) {
                    for (String sct : StringUtils.split(summaryCoverageThreshold, ",")) {
                        builder.addArgument(GATKDepthOfCoverageCLI.SUMMARYCOVERAGETHRESHOLD, sct);
                    }
                }

                CondorJob gatkGeneDepthOfCoverageJob = builder.build();
                logger.info(gatkGeneDepthOfCoverageJob.toString());
                graph.addVertex(gatkGeneDepthOfCoverageJob);

            } catch (Exception e) {
                throw new WorkflowException(e);
            }

        }

        return graph;
    }

    @Override
    public void postRun() throws WorkflowException {
        logger.info("ENTERING postRun()");

        File mapseqrc = new File(System.getProperty("user.home"), ".mapseqrc");
        Executor executor = BashExecutor.getInstance();

        SampleDAO sampleDAO = getWorkflowBeanService().getMaPSeqDAOBean().getSampleDAO();

        Set<Sample> sampleSet = getAggregatedSamples();

        Workflow ncgenesWorkflow = null;
        try {
            ncgenesWorkflow = getWorkflowBeanService().getMaPSeqDAOBean().getWorkflowDAO().findByName("NCGenes").get(0);
        } catch (MaPSeqDAOException e1) {
            e1.printStackTrace();
        }

        WorkflowRun workflowRun = getWorkflowRunAttempt().getWorkflowRun();

        for (Sample sample : sampleSet) {

            if ("Undetermined".equals(sample.getBarcode())) {
                continue;
            }

            File outputDirectory = new File(sample.getOutputDirectory(), getName().replace("DOC", ""));

            String prefix = null;
            Set<Attribute> attributeSet = workflowRun.getAttributes();
            if (attributeSet != null && !attributeSet.isEmpty()) {
                Iterator<Attribute> attributeIter = attributeSet.iterator();
                while (attributeIter.hasNext()) {
                    Attribute attribute = attributeIter.next();
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

            Set<FileData> fileDataSet = sample.getFileDatas();

            Set<String> attributeNameSet = new HashSet<String>();

            for (Attribute attribute : attributeSet) {
                attributeNameSet.add(attribute.getName());
            }

            Set<String> synchSet = Collections.synchronizedSet(attributeNameSet);

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
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals(totalCoverageKey)) {
                                            attribute.setValue(split[1]);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute(totalCoverageKey, split[1]));
                                }

                                String meanCoverageKey = String.format("GATKDepthOfCoverage.%s.mean", prefix);
                                if (synchSet.contains(meanCoverageKey)) {
                                    for (Attribute attribute : attributeSet) {
                                        if (attribute.getName().equals(meanCoverageKey)) {
                                            attribute.setValue(split[1]);
                                            break;
                                        }
                                    }
                                } else {
                                    attributeSet.add(new Attribute(meanCoverageKey, split[2]));
                                }
                            }
                        }
                    }
                } catch (IOException e1) {
                    e1.printStackTrace();
                }

            }

            Long totalPassedReads = null;
            for (Attribute attribute : attributeSet) {
                if ("SAMToolsFlagstat.totalPassedReads".equals(attribute.getName())) {
                    totalPassedReads = Long.valueOf(attribute.getValue());
                    break;
                }
            }

            if (totalPassedReads == null) {
                logger.warn("SAMToolsFlagstat.totalPassedReads is null");
            }

            potentialFileList = FileUtils.listFiles(
                    outputDirectory,
                    FileFilterUtils.and(FileFilterUtils.prefixFileFilter(prefix),
                            FileFilterUtils.suffixFileFilter(".sample_interval_summary")), null);

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
                        for (Attribute attribute : attributeSet) {
                            if (attribute.getName().equals(totalCoverageCountKey)) {
                                attribute.setValue(totalCoverageCount + "");
                                break;
                            }
                        }
                    } else {
                        attributeSet.add(new Attribute(totalCoverageCountKey, totalCoverageCount + ""));
                    }

                    String numberOnTargetKey = String.format("%s.numberOnTarget", prefix);

                    if (totalPassedReads != null) {
                        if (synchSet.contains(numberOnTargetKey)) {
                            for (Attribute attribute : attributeSet) {
                                if (attribute.getName().equals(numberOnTargetKey) && totalPassedReads != null) {
                                    attribute.setValue((double) totalCoverageCount / (totalPassedReads * 100) + "");
                                    break;
                                }
                            }
                        } else {
                            attributeSet.add(new Attribute(numberOnTargetKey, (double) totalCoverageCount
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
                sample.setAttributes(attributeSet);
                sampleDAO.save(sample);
            } catch (MaPSeqDAOException e) {
                e.printStackTrace();
            }

            File bamFile = null;

            List<File> potentialBAMFileList = WorkflowUtil.lookupFileByJobAndMimeTypeAndWorkflowId(fileDataSet,
                    getWorkflowBeanService().getMaPSeqDAOBean(), GATKTableRecalibration.class,
                    MimeType.APPLICATION_BAM, ncgenesWorkflow.getId());

            // assume that only one GATKTableRecalibration job exists
            if (potentialBAMFileList.size() > 0) {
                bamFile = potentialBAMFileList.get(0);
            }

            if (bamFile == null) {
                logger.error("bam file to process was not found");
                throw new WorkflowException("bam file to process was not found");
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

}
