package main;

import com.google.common.base.Joiner;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.algorithm_execution.FileGenerator;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.InclusionDependencyAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.TempFileAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.result_receiver.InclusionDependencyResultReceiver;
import de.metanome.backend.algorithm_execution.TempFileGenerator;
import main.SpiderConfiguration.SpiderConfigurationBuilder;
import de.metanome.util.TPMMSConfiguration;
import de.metanome.util.TPMMSConfigurationRequirements;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public abstract class SpiderAlgorithm implements InclusionDependencyAlgorithm,
    IntegerParameterAlgorithm,
    BooleanParameterAlgorithm,
    TempFileAlgorithm {

  final SpiderConfigurationBuilder builder;
  final TPMMSConfiguration tpmmsConfiguration;
  final SpiderConfiguration defaultValues;
  final Spider spider;

  SpiderAlgorithm() {
    builder = SpiderConfiguration.builder();
    tpmmsConfiguration = new TPMMSConfiguration();
    defaultValues = SpiderConfiguration.withDefaults();
    spider = new Spider();
  }

  List<ConfigurationRequirement<?>> common() {
    final List<ConfigurationRequirement<?>> requirements = new ArrayList<>();
    requirements.add(processEmptyColumns());
    requirements.addAll(TPMMSConfigurationRequirements.tpmms());
    return requirements;
  }

  private ConfigurationRequirement<?> processEmptyColumns() {
    final ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(
        ConfigurationKey.PROCESS_EMPTY_COLUMNS.name());
    requirement.setDefaultValues(new Boolean[]{defaultValues.isProcessEmptyColumns()});
    return requirement;
  }

  @SafeVarargs
  final <T> void handleUnknownConfiguration(final String identifier, final T... values)
      throws AlgorithmConfigurationException {

    final String formattedValues = Joiner.on(", ").join(values);
    final String message = String
        .format("Unknown configuration '%s', values: '%s'", identifier, formattedValues);
    throw new AlgorithmConfigurationException(message);
  }

  @Override
  public void execute() throws AlgorithmExecutionException {
    SpiderConfiguration configuration = null;
    try {
      configuration = builder.tpmmsConfiguration(tpmmsConfiguration)
              .tempFileGenerator(new TempFileGenerator())
            .build();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    spider.execute(configuration);
  }

  @Override
  public void setResultReceiver(
      final InclusionDependencyResultReceiver inclusionDependencyResultReceiver) {

    builder.resultReceiver(inclusionDependencyResultReceiver);
  }

  @Override
  public void setIntegerConfigurationValue(final String identifier, final Integer... values)
      throws AlgorithmConfigurationException {

    if (TPMMSConfigurationRequirements.acceptInteger(identifier, values, tpmmsConfiguration)) {
      return;
    }

    handleUnknownConfiguration(identifier, values);
  }

  @Override
  public void setBooleanConfigurationValue(final String identifier, final Boolean... values)
      throws AlgorithmConfigurationException {

    if (identifier.equals(ConfigurationKey.PROCESS_EMPTY_COLUMNS.name())) {
      builder.processEmptyColumns(values[0]);
    } else {
      handleUnknownConfiguration(identifier, values);
    }
  }

  @Override
  public void setTempFileGenerator(final FileGenerator tempFileGenerator) {
    builder.tempFileGenerator(tempFileGenerator);
  }

  @Override
  public String getAuthors() {
    return "Falco Dürsch, Tim Friedrich";
  }

  @Override
  public String getDescription() {
    return "Sort-Merge-Join IND discovery";
  }
}
