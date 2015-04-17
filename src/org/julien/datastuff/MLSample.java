import com.amazonaws.AmazonClientException;
import com.amazonaws.services.machinelearning.AmazonMachineLearningClient;
import com.amazonaws.services.machinelearning.model.DescribeMLModelsResult;
import com.amazonaws.services.machinelearning.model.MLModel;
import com.amazonaws.services.machinelearning.model.PredictRequest;
import com.amazonaws.services.machinelearning.model.PredictResult;
import com.amazonaws.services.machinelearning.model.RealtimeEndpointInfo;

public class MLSample {

	public static void main(String[] args) {

		// Create a machine learning client
		AmazonMachineLearningClient client = new AmazonMachineLearningClient();

		// Get list of prediction models
		DescribeMLModelsResult models = client.describeMLModels();

		// Show basic information about each model
		for (MLModel m : models.getResults()) {
			System.out.println("Model name: " + m.getName());
			System.out.println("Model id: " + m.getMLModelId());
			System.out.println("Model status: " + m.getStatus());

			RealtimeEndpointInfo endpoint = m.getEndpointInfo();
			System.out.println("Endpoint URL: " + endpoint.getEndpointUrl());
			System.out.println("Endpoint status: "
					+ endpoint.getEndpointStatus());
		}

		// Select first model
		MLModel model = models.getResults().get(0);

		// Build a prediction request
		PredictRequest request = new PredictRequest();
		// Select prediction model
		request.setMLModelId(model.getMLModelId());
		// Select realtime endpoint
		request.setPredictEndpoint(model.getEndpointInfo().getEndpointUrl());

		// Build data to be predicted
		request.addRecordEntry("lastname", "Simon")
		.addRecordEntry("firstname", "Julien")
		.addRecordEntry("age", "44").addRecordEntry("gender", "M")
		.addRecordEntry("state", "Texas").addRecordEntry("month", "4")
		.addRecordEntry("day", "106").addRecordEntry("hour", "10")
		.addRecordEntry("minutes", "26").addRecordEntry("items", "5");

		System.out.println("Sending prediction request for: "
				+ request.getRecord());

		// Send prediction request
		PredictResult result;
		try {
			long start = System.currentTimeMillis();
			result = client.predict(request);
			long end = System.currentTimeMillis();
			System.out.println((end - start) + " ms");
		} catch (Exception e) {
			throw new AmazonClientException("Prediction failed", e);
		}

		// Display predicted value
		System.out.println("Predicted value:"
				+ result.getPrediction().getPredictedValue());
	}
}
