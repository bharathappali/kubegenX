import { SegmentedControl, Text, Tooltip } from "@mantine/core";
import { Icon } from "./icons";

const TIP = "XHigh uses 80% of the free CPU and memory. High is 40%, Medium 15%, and Low 5%, with at least 2 workers. A generation worker requests 500m CPU and 1Gi. A conversion worker requests 2 CPU and 4Gi. Neither has a limit. Generation waits while a conversion is running, and a conversion waits while data is being generated.";

export function EffortField({ value, onChange }: { value: string; onChange: (v: string) => void }) {
  return (
    <div>
      <div className="effort-label">
        <Text size="sm" fw={700}>Effort</Text>
        <Tooltip label={TIP} multiline w={280} withArrow position="top-start">
          <button type="button" className="icon-btn" aria-label="About effort"><Icon name="info" /></button>
        </Tooltip>
      </div>
      <div className="tabs-center">
        <SegmentedControl
          value={value}
          onChange={onChange}
          data={[
            { label: "Low", value: "low" },
            { label: "Medium", value: "medium" },
            { label: "High", value: "high" },
            { label: "XHigh", value: "xhigh" },
          ]}
          color="slate"
        />
      </div>
    </div>
  );
}
