namespace Heimdall.Shared.Cluster;

/// <summary>
/// Turns a key's preference list into an ordered list of read candidates.
///
/// The ring already says which nodes hold a video. The load balancer decides which of them to
/// *ask*, and in what order:
/// <list type="number">
///   <item>Take the replicas (preference list minus the primary) and select
///   <c>ceil(replicaCount * ReadReplicaPercentage / 100)</c> of them.</item>
///   <item>Rotate the selection window per request so consecutive reads of the same hot video land
///   on different replicas. The rotation counter is process-local, so there is no shared state and
///   no central lookup table - every gateway instance spreads its own traffic.</item>
///   <item>Append the primary, then any replicas that were not selected, so the full preference
///   list stays available as a failover chain.</item>
/// </list>
/// A percentage of 0 degenerates to "always read from the primary, fail over to replicas".
/// </summary>
public sealed class ReplicaLoadBalancer
{
    private readonly int _readReplicaPercentage;
    private int _rotation = -1;

    public ReplicaLoadBalancer(int readReplicaPercentage)
    {
        if (readReplicaPercentage is < 0 or > 100)
            throw new ArgumentOutOfRangeException(nameof(readReplicaPercentage), "Read replica percentage must be between 0 and 100.");
        _readReplicaPercentage = readReplicaPercentage;
    }

    public int ReadReplicaPercentage => _readReplicaPercentage;

    /// <summary>How many of <paramref name="replicaCount"/> replicas get selected for reads.</summary>
    public int ReplicaSelectionSize(int replicaCount)
    {
        if (replicaCount <= 0 || _readReplicaPercentage == 0) return 0;
        var selected = (int)Math.Ceiling(replicaCount * _readReplicaPercentage / 100.0);
        return Math.Clamp(selected, 1, replicaCount);
    }

    /// <summary>
    /// Ordered read candidates for <paramref name="preferenceList"/>. The returned list always
    /// contains every node from the preference list exactly once, so the caller can walk it as a
    /// failover chain and only give up once every holder of the video has been tried.
    /// </summary>
    public IReadOnlyList<NodeDescriptor> SelectReadCandidates(IReadOnlyList<NodeDescriptor> preferenceList)
        => SelectReadCandidates(preferenceList, Interlocked.Increment(ref _rotation));

    /// <summary>Deterministic overload, used by the tests and by callers that want a fixed rotation.</summary>
    public IReadOnlyList<NodeDescriptor> SelectReadCandidates(IReadOnlyList<NodeDescriptor> preferenceList, int rotation)
    {
        ArgumentNullException.ThrowIfNull(preferenceList);
        if (preferenceList.Count <= 1) return preferenceList;

        var primary = preferenceList[0];
        var replicaCount = preferenceList.Count - 1;
        var take = ReplicaSelectionSize(replicaCount);

        var ordered = new List<NodeDescriptor>(preferenceList.Count);
        if (take == 0)
        {
            ordered.Add(primary);
            for (var i = 1; i < preferenceList.Count; i++)
                ordered.Add(preferenceList[i]);
            return ordered;
        }

        // Rotation is non-negative even if the counter has wrapped into negative territory.
        var offset = (int)((uint)rotation % (uint)replicaCount);

        var selected = new List<NodeDescriptor>(take);
        for (var i = 0; i < take; i++)
            selected.Add(preferenceList[1 + ((offset + i) % replicaCount)]);

        ordered.AddRange(selected);
        ordered.Add(primary);

        var selectedIds = selected.Select(static n => n.Id).ToHashSet(StringComparer.Ordinal);
        for (var i = 1; i < preferenceList.Count; i++)
        {
            if (!selectedIds.Contains(preferenceList[i].Id))
                ordered.Add(preferenceList[i]);
        }

        return ordered;
    }
}
