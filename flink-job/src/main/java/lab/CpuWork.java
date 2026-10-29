package lab;

public final class CpuWork {
    private CpuWork() {}
    public static long payloadSeed(String payload, long userId) {
        long hash = 0xcbf29ce484222325L ^ userId;
        for (int i = 0; i < payload.length(); i++) hash = (hash ^ payload.charAt(i)) * 0x100000001b3L;
        return hash;
    }
    /** Integer overflow is intentional. The result is carried downstream and consumed by the sink. */
    public static long compute(long seed, int iterations) {
        if (iterations < 0) throw new IllegalArgumentException("Negative iterations");
        long x = seed;
        for (int i = 0; i < iterations; i++) {
            x ^= x >>> 12;
            x ^= x << 25;
            x ^= x >>> 27;
            x = Long.rotateLeft(x * 0x2545f4914f6cdd1dL + i, 17);
        }
        return x;
    }
}
